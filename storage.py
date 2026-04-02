import os

print("\n-----------------------")
print("Shuru Project - Storage")
print("-----------------------")

#TASK 4: Saving/writing the transformed data into the processed folder in the parquet format
def fifa_storage(fifa_transed):
    print(r"Writing the processed data to D:\Big_Data_Engineering\Interviews\Shuru Coding Round\processed_parquet...")
    output_path = "D:\\Big_Data_Engineering\\Interviews\\Shuru Coding Round\\processed_parquet"
    print("To check whether the path exists. If True, cool!: ", os.path.exists("D:\\Big_Data_Engineering\\Interviews\\Shuru Coding Round"))
    fifa_transed.write.mode("overwrite").partitionBy("team", "age_group").parquet(output_path)
    print("Data stored as Parquet successfully!")
    #Note: While writing into the parquet, I faced the error. The error is due to the partitionBy "team".
    # In the dataset, the "team" column contains special and unnecessary characters. We need to clean them properly using "regexp_replace"