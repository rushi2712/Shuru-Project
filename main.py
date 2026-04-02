print("\n--------------------")
print("Shuru Project - MAIN")
print("--------------------")

#This is the main file which executes all the operations from here
from ingestion import read_data
from data_cleaning import fifa_clean
from transformation import fifa_trans
from storage import fifa_storage

#Step 1: INGESTION
fifa_read, spark= read_data()

#Step 2: DATA_CLEANING
fifa_cleaned = fifa_clean(fifa_read)

#Step 3: TRANSFORMATION
fifa_transformed = fifa_trans(fifa_cleaned)

#Step 4: STORAGE
fifa_storage(fifa_transformed)