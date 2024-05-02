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
        # Connect to MongoDB
        client = pymongo.MongoClient("mongodb://sahithi:sahithimongo@localhost:27017/admin")
        db = client["Dapmongo"]

        # Retrieve and convert data from "enforcement" collection
        enforcement_data = list(db["enforcement"].find())
        enforcement_df = pd.DataFrame(enforcement_data)
        context.log.info("Fetched and converted enforcement data.")

        # Retrieve and convert data from "occupancy" collection
        occupancy_data = list(db["occupancy"].find())
        occupancy_df = pd.DataFrame(occupancy_data)
        context.log.info("Fetched and converted occupancy data.")

        # Output the dataframes
        return Output(enforcement_df, "enforcement_df"), Output(occupancy_df, "occupancy_df")

    except pymongo.errors.ConnectionError as e:
        context.log.error(f"Failed to connect to MongoDB: {e}")
        raise
    except Exception as e:
        context.log.error(f"An error occurred: {e}")
        raise