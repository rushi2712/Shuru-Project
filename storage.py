import os

print("\n-----------------------")
print("Shuru Project - Storage")
print("-----------------------")

print("To check whether the path (D:\\Users\\....\\Shuru_project) exists. If True, cool!: ", os.path.exists("D:\\Users\\1767\\Desktop\\Cloud Data Engineering\\Projects\\Shuru_project"))

#TASK 4: Saving/writing the transformed data into the processed and queries folder in the parquet format
def store_transformed_data(df):
    output_path_1 = "D:\\Users\\1767\\Desktop\\Cloud Data Engineering\\Projects\\Shuru_project\\processed_parquet"

    print(r"Writing the Transformed data to D:\Users\...\processed_parquet")
    df.write.mode("overwrite").partitionBy("team", "age_group").parquet(output_path_1)
    print("Transformed Data stored as Parquet successfully!")

def store_query_output(df, folder_name):
    base_path = "D:\\Users\\1767\\Desktop\\Cloud Data Engineering\\Projects\\Shuru_project\\queries_parquet"
    output_path_2 = f"{base_path}\\{folder_name}"

    print(r"\nWriting the query output to D:\Users\...\queries_parquet")
    df.write.mode("overwrite").parquet(output_path_2)
    print("Query Data stored as Parquet successfully!")

#def store_query_output(result_q1, q2_best_teams):
#    base_path = "D:\\Users\\1767\\Desktop\\Cloud Data Engineering\\Projects\\Shuru_project\\queries_parquet"
#    output_path_2 = f"{base_path}\\{folder_name}"

#    print("Writing the query output to D:\Users\...\queries_parquet\q2_best_teams")
#    result_q1.write.mode("overwrite").parquet(output_path_2)
#    print("Q2 Data stored as Parquet successfully!")

#Note: While writing into the parquet, I faced the error. The error is due to the partitionBy "team".
# In the dataset, the "team" column contains special and unnecessary characters. We need to clean them properly using "regexp_replace"