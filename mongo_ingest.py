import pandas as pd
from pymongo import MongoClient
 
# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['dapdatabase']  # Replace 'your_database' with your database name
collection = db['title']  # Replace 'your_collection' with your collection name
 
# Read CSV file into a pandas DataFrame
df = pd.read_csv(r"C:\Users\venna\Desktop\DAP project\Electric_Vehicle_Title_and_Registration_Activity.csv")  # Replace 'your_file.csv' with the path to your CSV file
 
# Convert DataFrame to a list of dictionaries (one dictionary per row)
data = df.to_dict(orient='records')
 
collection.insert_many(data)
 
print("Data inserted successfully.")