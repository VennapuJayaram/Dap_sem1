import json
from pymongo import MongoClient
from dagster import op, Out, job , repository
import pandas as pd
import logging




@op
def ingesting_dataset_1() -> bool:
    file_path = "Enforcement.json"
    client = MongoClient('mongodb://sahithi:sahithimongo@localhost:27017/admin')
    db = client['Dapmongo']  # Replace 'your_database' with your database name
    collection =  db['enforcement']

    try:
        with open(file_path, 'r') as file:
            full_data = json.load(file)
        logging.info("File loaded successfully.")

        data_entries = full_data['data']
    except FileNotFoundError:
        logging.error("File not found.")

        return False
    except json.JSONDecodeError:
        logging.error("Error decoding JSON.")
        return False
    except KeyError:
        logging.error("Key error in accessing data.")
        return False

    data_dicts = []
    for entry in data_entries:
        try:
            data_dict = {
                "rowid": entry[0],
                "guid": entry[1],
                "data1": entry[2],
                "data2": entry[3],
                "data3": entry[4],
                "data4": entry[5],
                "data5": entry[6],
                "data6": entry[7],
                "Case_Number": entry[8],
                "LADBS_Inspection_District": entry[9],
                "Address_House_Number": entry[10],
                "Address_House_Fraction_Number": entry[11],
                "Address_Street_Direction": entry[12],
                "Address_Street_Name": entry[13],
                "Address_Street_Suffix": entry[14],
                "Address_Street_Suffix_Direction": entry[15],
                "Address_Zip": entry[16],
                "Date_Case_Generated": entry[17],
                "Date_Case_Closed": entry[18],
                "Parcel_Identification_Number_PIN": entry[19],
                "Case_Type": entry[20],
                "Area_Planning_Commission_APC": entry[21],
                "Status_of_Case": entry[22],
                "latitude": entry[23][1] if entry[23] is not None else None,
                "longitude": entry[23][2] if entry[23] is not None else None,
            }
        except IndexError:
            logging.error("Index error in processing data entries.")
            return False
        data_dicts.append(data_dict)

    # Assuming `collection` is predefined somewhere else and accessible here
    try:
        collection.insert_many(data_dicts)
        logging.info(f"Inserted {len(data_dicts)} records into MongoDB.")
    except Exception as e:
        logging.error(f"Exception during MongoDB insert: {str(e)}")
        # Handling any exception that might occur during the database operation
        return False
    logging.info("Data ingestion completed successfully.")
    return True  # Return True if all steps completed successfully
