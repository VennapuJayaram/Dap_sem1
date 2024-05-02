from dagster import op, Out, Output, DynamicOut, DynamicOutput, OpExecutionContext , In, job
import pymongo
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DatabaseError
from ingesting_datasets import *

@op(
    ins={"dataset1": In(), "dataset2": In()},
    out={
        "enforcement_df": Out(dagster_type=pd.DataFrame),
        "occupancy_df": Out(dagster_type=pd.DataFrame)
    }
)
def extract_mongo_data(context: OpExecutionContext,  dataset1, dataset2 ):
    try:
        # Connection to MongoDB
        client = pymongo.MongoClient("mongodb://sahithi:sahithimongo@localhost:27017/admin")
        db = client["Dapmongo"]

        # Retrieving and converting data from "enforcement" collection
        enforcement_data = list(db["enforcement"].find())
        enforcement_df = pd.DataFrame(enforcement_data)
        context.log.info("Retrieveing and converting data from enforcement")

        # Retrieving and converting data from "occupancy" collection
        occupancy_data = list(db["occupancy"].find())
        occupancy_df = pd.DataFrame(occupancy_data)
        context.log.info("Retrieveing and converting data from occupancy")

        # The actual Output of the dataframes
        return Output(enforcement_df, "enforcement_df"), Output(occupancy_df, "occupancy_df")

    except pymongo.errors.ConnectionError as e:
        context.log.error(f"Failed to connect to MongoDB: {e}")
        raise
    except Exception as e:
        context.log.error(f"An error occurred: {e}")
        raise

@op(
    ins={
        "enforcement_df": In(dagster_type=pd.DataFrame),
        "occupancy_df": In(dagster_type=pd.DataFrame)
    }
)
def transform_and_load(context: OpExecutionContext, enforcement_df: pd.DataFrame, occupancy_df: pd.DataFrame):
    context.log.info("transforming and loading")
    
    
    
    try:
        # Stripping whitespace from column names
        enforcement_df.columns = enforcement_df.columns.str.strip()
        occupancy_df.columns = occupancy_df.columns.str.strip()
        context.log.info("Column names stripped.")

        # Defining  which columns do we need to drop
        columns_to_drop_in_enforcement = [ "_id" ,"rowid" , "guid" , "data1" , "data2" ,"data3" , "data4" , "data5" ,
                                        "data6" , "Address_House_Number","Address_House_Fraction_Number", 
                                        "Address_Street_Direction", 
        "Address_Street_Name","Address_Street_Suffix", "Address_Street_Suffix_Direction", "Date_Case_Closed"]
        
        columns_to_drop_in_occupancy = ["_id","Assessor Book","Assessor Page","Assessor Parcel","TRACT","BLOCK","LOT",
              "Reference # (Old Permit #)","PCIS Permit #","Permit Sub-Type",
              "Permit Category","Initiating Office","Address Start","Address Fraction Start",
              "Address End","Address Fraction End","Street Direction","Street Name","Street Suffix",
              "Suffix Direction","Unit Range Start","Unit Range End","Work Description",
              "Floor Area-L.A. Zoning Code Definition","# of Residential Dwelling Units","Contractor's Business Name",
              "Contractor Address","Contractor City",
              "Contractor State","License Type","License #","Principal First Name",
              "Principal Middle Name","Principal Last Name","License Expiration Date",
              "Applicant First Name","Applicant Last Name","Applicant Business Name","Event Code",
              "Applicant Address 1","Applicant Address 2","Applicant Address 3" , "Project Number" ]

        # Dropping unnecessary columns which we dont require
        enforcement_df.drop(columns=columns_to_drop_in_enforcement, inplace=True)
        occupancy_df.drop(columns=columns_to_drop_in_occupancy, inplace=True)
        context.log.info("dropping unnecessary columns")

        # Handling integer columns
        int_columns_enforcement = ["Case_Number", "LADBS_Inspection_District", "Address_Zip"]
        for col in int_columns_enforcement:
            enforcement_df[col] = enforcement_df[col].fillna(0).astype(int)
        
        int_columns_occupancy = ["CofO Number", "Zip Code"]
        for col in int_columns_occupancy:
            occupancy_df[col] = occupancy_df[col].fillna(0).astype(int)
        context.log.info("Handeled integer columns")

        # Handling float columns
        float_columns_enforcement = ["latitude", "longitude"]
        for col in float_columns_enforcement:
            enforcement_df[col] = enforcement_df[col].fillna(0.0).astype(float)
        
        
        context.log.info("Handling float columns")

        # Splitting and converting latitude/longitude coordinates
        occupancy_df[['latitude', 'longitude']] = occupancy_df['Latitude/Longitude'].str.replace(r'\(|\)', '', regex=True).str.split(',', expand=True)
        occupancy_df['latitude'] = occupancy_df['latitude'].astype(float)
        occupancy_df['longitude'] = occupancy_df['longitude'].astype(float)
        occupancy_df.drop(columns=['Latitude/Longitude'], inplace=True)
        float_columns_occupancy = ["Floor Area-L.A. Building Code Definition", "latitude", "longitude"]
        for col in float_columns_occupancy:
            occupancy_df[col] = occupancy_df[col].fillna(0.0).astype(float)
        context.log.info("Split and converted latitude/longitude fields.")

        # Converting datetime columns
        #converting data column into datetime format in occupancy dataframe for further analysis
        occupancy_df["CofO Issue Date"] = pd.to_datetime(occupancy_df["CofO Issue Date"], format='%m/%d/%Y')
        occupancy_df["Status Date"] = pd.to_datetime(occupancy_df["Status Date"])
        occupancy_df["Status Date"] = occupancy_df["Status Date"].dt.date
        occupancy_df["Permit Issue Date"] = pd.to_datetime(occupancy_df["Permit Issue Date"], format='%m/%d/%Y').dt.date
        #Converting two digit date format to four digit date formate and convert to datetime objects
        enforcement_df["Date_Case_Generated"] = enforcement_df["Date_Case_Generated"].apply(lambda x: "20" + x[2:] if x.startswith("00") else x)
        enforcement_df["Date_Case_Generated"] = pd.to_datetime(enforcement_df["Date_Case_Generated"], format="%Y-%m-%dT%H:%M:%S")
                
        context.log.info("Converted datetime columns.")

        # Renaming overlapped columns before merging
        common_columns = enforcement_df.columns.intersection(occupancy_df.columns)
        enforcement_df.rename(columns={col: f'enforcement_{col}' for col in common_columns}, inplace=True)
        occupancy_df.rename(columns={col: f'occupancy_{col}' for col in common_columns}, inplace=True)
        context.log.info("Renamed overlapping columns for clarity.")
    
        # Combining dataframes
        combined_df = pd.concat([occupancy_df, enforcement_df], ignore_index=True)
        context.log.info("Combined dataframes into a single DataFrame.")

        engine = create_engine('postgresql://postgres:dapdb7@localhost:5432/postgres')
        # Attempting to create the database if it doesn't exist in the records
        try:
            with engine.connect() as connection:
                connection.execution_options(isolation_level="AUTOCOMMIT")
                connection.execute(text("CREATE DATABASE dapdata"))
        except DatabaseError as e:
            if "already exists" in str(e):
                context.log.info("Database 'dapdata' already exists.")
            else:
                raise
        # Connecting to the newly created or if existing database
        engine = create_engine('postgresql://postgres:dapdb7@localhost:5432/dapdata')
        try:
            with engine.connect() as connection:
                connection.execution_options(isolation_level="AUTOCOMMIT")
                # Assuming 'combined_df' is a DataFrame  which contains the data to be loaded
                combined_df.to_sql('datatable', connection, if_exists='replace', index=False)
                context.log.info("Data loaded to PostgreSQL database 'dapdata' in table 'datatable'.")
        except Exception as e:
            context.log.error(f"An error occurred during data load: {e}")
            raise
    except Exception as e:
        context.log.error(f"An error occurred during transformation and load: {e}")
        raise    
