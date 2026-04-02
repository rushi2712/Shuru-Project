print("\n------------------------")
print("Shuru Project - cleaning")
print("------------------------")

from pyspark.sql.types import StringType
from pyspark.sql.functions import *

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

def fifa_clean(fifa_read):
    #1. Replace the NULL, "", Fake nulls with None
    #Also, replace the special characters with "" for specific column(s)
    fifa_cleaned = fifa_read.replace(["Null", "na", "null", ""], None)
    print("After replacing the null values with None:")
    fifa_cleaned.show(5)

    #2. Trimming extra spaces
    #Actually, we are doing a Pro level trimming, below. It looks complex. But once understood, it feels understandable.
    #While trimming, we trim only the String type. So, when we come across any int datatype or date datatype columns, we avoid trimming them.
    fifa_cleaned = fifa_cleaned.select([
        trim(regexp_replace(col(c), " +", " ")).alias(c)    #3.1. (regexp_replace(col(c)), " +", " ") replaces multiple spaces with single space. 3.2 Then trim removes spaces at front and back.
        if isinstance(fifa_cleaned.schema[c].dataType, StringType) else col(c)  #2. Meaning: Is this column a String? If not, at avoids that column
        for c in fifa_cleaned.columns   #1. This for loop goes through all the table columns
    ]).withColumn("team", trim(regexp_replace(col("team"), r"[\\/:*?\"'<1.>|]", ""))) #4. Here, we are specifically cleaning the "team" column as it contains special characters.
    print("After Trimming the extra spaces and cleaning the special characters in team column, the DF is")
    fifa_cleaned.show(5)
    fifa_cleaned.printSchema()

    #3. dropDuplicates the records in the table
    fifa_cleaned = fifa_cleaned.dropDuplicates()
    print("After dropping the duplicate records, the dataframe is")
    fifa_cleaned.show(5)

    #To verify the count after dropping duplicates, before dropping duplicates(17981)
    fifa_count_after_dd = fifa_cleaned.select([
        count(when(col("player_id").isNotNull(), "player_id")).alias("player_id")
                                             ])
    print("The count after dropping duplicates: ")
    fifa_count_after_dd.show()

    #4. Standardization i.e. converting the Title case, upper case, and all other mixed cases to lower.
    cols_to_lower = ["name", "nationality", "team"]
    fifa_cleaned = fifa_cleaned.select([
        lower(col(c)).alias(c)
        if c in cols_to_lower else col(c)
        for c in fifa_cleaned.columns
    ]).orderBy(asc("player_id"))
    print("Converting the name, nationality and team values to lower case")
    fifa_cleaned.show(5)

    return fifa_cleaned