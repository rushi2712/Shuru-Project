import os
import urllib.request
import ssl

from pyspark.sql.functions import explode, isnull
from pyspark.sql.types import StringType

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt":os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe":os.path.join(hadoop_home, "bin", "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll":os.path.join(hadoop_home, "bin", "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
#import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ["HADOOP_HOME"] = r"C:\hadoop"
#os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\ptrus\.jdks\corretto-1.8.0_472'        #  <----- 🔴JAVA PATH🔴
# Set HADOOP_HOME to the folder ABOVE the bin folder
# Add the bin folder to your System Path
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.appName("FIFA Pipeline").getOrCreate()
# 🔥 Important setting
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#Enabling dynamic partition overwrite using Spark config. So, only affected partitions are updated instead of rewriting the entire dataset.
#THis will be useful while REWRITING(re-saving) by partitions after transformations. Without this, whenever we re-write the entire transformations,
#the partitioned folders will be created again and again at the destination path, thus results in more cost, concurrent issues, and any other risks while dealing with huge data
#=============================================================#
#Write code from below
print("\n-------------")
print("Shuru Project")
print("-------------")

#######################
#TASK 1: DATA INGESTION - Reading the CSV file - FIFA-21 Complete
print("\nThe FIFA dataset is:")
#fifa = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("sep", ";").load(r"D:\FIFA-21 Complete.csv")
#OR
fifa = spark.read.csv(r"D:\Big_Data_Engineering\Interviews\Shuru Coding Round\raw\FIFA-21 Complete.csv", header=True, inferSchema=True, sep=";")
fifa.show(10)
fifa.printSchema()

######################
#TASK 2: DATA CLEANING
#The first and the foremost check is to find the empty cells manually in the Raw CSV file, by applying the filters.
#Next, if you are not clear with the manual check, with PySpark, we can find the count of empty cells. But this consumes time.

#Let's find the count of blank rows in player_id column
'''
fifa = fifa.select([count(when(
                            col("player_id").isNull(), "player_id")).alias("player_id")
])
print("The blank values count is: ")
fifa.show()'''

#Even though we know that there are no NULL values or empty strings "" or fake nulls (NA, null) in the Raw CSV file,
#1. Replace the NULL, "", Fake nulls with None
#2. Let's trim the cells as there might be empty spaces in the strings ("Rushi" != "Rushi "), ("   Rushi    PT ")
#3. dropDuplicates the records in the table
#4. Standardization
#These are sufficient

#1. Replace the NULL, "", Fake nulls with None
#Also, replace the special characters with "" for specific column(s)
fifa_clean = fifa.replace(["Null", "na", "null", ""], None)
print("After replacing the null values with None:")
fifa_clean.show(5)

#2. Trimming extra spaces
#Actually, we are doing a Pro level trimming, below. It looks complex. But once understood, it feels understandable.
#While trimming, we trim only the String type. So, when we come across any int datatype or date datatype columns, we avoid trimming them.
fifa_clean = fifa_clean.select([
    trim(regexp_replace(col(c), " +", " ")).alias(c)    #3.1. (regexp_replace(col(c)), " +", " ") replaces multiple spaces with single space. 3.2 Then trim removes spaces at front and back.
    if isinstance(fifa_clean.schema[c].dataType, StringType) else col(c)  #2. Meaning: Is this column a String? If not, at avoids that column
    for c in fifa.columns   #1. This for loop goes through all the table columns
]).withColumn("team", trim(regexp_replace(col("team"), r"[\\/:*?\"'<1.>|]", ""))) #Here, we are specifically cleaning the "team" column as it contains special characters.

print("After Trimming the extra spaces and cleaning the special characters in team column, the DF is")
fifa_clean.show(5)
fifa_clean.printSchema()

#3. dropDuplicates the records in the table
fifa_clean = fifa_clean.dropDuplicates()
print("After dropping the duplicate records, the dataframe is")
fifa_clean.show(5)
#To verify the count after dropping duplicates, before dropping duplicates(17981)
fifa_count_after_dd = fifa_clean.select([count(when(
    col("player_id").isNotNull(), "player_id")).alias("player_id")
])
print("The count after dropping duplicates: ")
fifa_count_after_dd.show()

#4. Standardization i.e. converting the Title case, upper case, and all other mixed cases to lower.
cols_to_lower = ["name", "nationality", "team"]
fifa_clean = fifa_clean.select([
    lower(col(c)).alias(c) if c in cols_to_lower else col(c)
    for c in fifa_clean.columns
]).orderBy(asc("player_id"))
print("Converting the name, nationality and team values to lower case")
fifa_clean.show(5)

#############################
#TASK 3: DATA TRANSFORMATIONS
#Transformation 1:Add a new column: growth_potential = (potential - overall)
#If growth_potential is negative, replace it with 0
fifa_trans = fifa_clean.withColumn("growth_potential",
                                 when(col("potential") - col("overall") > 0, col("potential") - col("overall"))
                                 .otherwise(0)
)
print("The Growth Potential (potential - overall) is:")
fifa_trans.orderBy(col("name")).show(10)

#Transformation 2: Add a new column age_group with brackets - <20, 20–25, 26–30, >30
print("The age brackets based on 'age' are: ")
fifa_trans = fifa_trans.withColumn("age_group",
                                 when(col("age") < 20, "<20")
                                 .when((col("age") >= 20) & (col("age") <= 25), "20-25")
                                 .when((col("age") >= 26) & (col("age") <= 30), "26-30")
                                 .otherwise(">30")
)
fifa_trans.orderBy(desc(col("player_id"))).show(10)

#TASK 4: Saving/writing the transformed data into the processed folder in the parquet format
print(r"Writing the processed data to D:\Big_Data_Engineering\Interviews\Shuru Coding Round\processed_parquet...")

output_path = "D:\\Big_Data_Engineering\\Interviews\\Shuru Coding Round\\processed_parquet"
print("To check whether the path exists. If True, cool.: ", os.path.exists("D:\\Big_Data_Engineering\\Interviews\\Shuru Coding Round"))
fifa_trans.write.mode("overwrite").partitionBy("team", "age_group").parquet(output_path)
print("Data stored as Parquet successfully!")
#Note: While writing into the parquet, I faced the error. The error is due to the partitionBy "team".
# In the dataset, the "team" column contains special and unnecessary characters. We need to clean them properly using "regexp_replace"