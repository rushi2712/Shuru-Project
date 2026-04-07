print("\n-------------------------------")
print("Shuru Project - Transformations")
print("-------------------------------")

from pyspark.sql.functions import *

#############################
#TASK 3: DATA TRANSFORMATIONS

def fifa_trans(fifa_cleaned):
    #Transformation 1:Add a new column: growth_potential = (potential - overall)
    #If growth_potential is negative, replace it with 0
    fifa_transed = fifa_cleaned.withColumn("growth_potential",
                                           when(col("potential") - col("overall") > 0,
                                                col("potential") - col("overall")).otherwise(0)
                                           )
    print("The Growth Potential (potential - overall) is:")
    fifa_transed.orderBy(col("name")).show(7)

    #Transformation 2: Add a new column age_group with brackets - <20, 20–25, 26–30, >30
    print("The age brackets based on 'age' are: ")
    fifa_transed = fifa_transed.withColumn("age_group",
                                           when(col("age") < 20, "<20")
                                           .when((col("age") >= 20) & (col("age") <= 25), "20-25")
                                           .when((col("age") >= 26) & (col("age") <= 30), "26-30")
                                           .otherwise(">30")
                                           )
    fifa_transed.orderBy(desc(col("player_id"))).show(10)

    return fifa_transed