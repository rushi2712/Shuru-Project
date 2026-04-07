print("\n--------------------")
print("Shuru Project - MAIN")
print("--------------------")

#This is the main file which executes all the operations from here
from ingestion import read_player_data
from data_cleaning import fifa_clean
from transformation import fifa_trans
from sql_queries.q1_top_players import top_5_players_sql
from sql_queries.q2_best_teams import highest_average_overall_23_sql
from storage import store_transformed_data, store_query_output

#Step 1: INGESTION
fifa_read, spark= read_player_data()

#Step 2: DATA_CLEANING
fifa_cleaned = fifa_clean(fifa_read)

#Step 3: TRANSFORMATION
fifa_transformed = fifa_trans(fifa_cleaned)

#Step 4.1: Create temp view for Query. 1 TempView is sufficient as we have only table
fifa_transformed.createOrReplaceTempView("fifa_players")
#Step 4.1.1: Query 1 - Top 5 players from each team
top_5_players = top_5_players_sql(spark)
print("Query 1: The top 5 players from each team based on growth_potential:")
top_5_players.show()

#Step 4.2: we can use the same TempView for the rest of the queries
#Step 4.1.2: Query 2 - Highest team average among players under 23 age
highest_average_overall_23 = highest_average_overall_23_sql(spark)
print("Query 2: The highest team average among players under 23 age")
highest_average_overall_23.show()

#Step 5: STORAGE of Transformed data
store_transformed_data(fifa_transformed)

#Step 6: Storage of Query data
store_query_output(df = top_5_players, folder_name = "q1_top_players")
store_query_output(df = highest_average_overall_23, folder_name = "q2_best_teams")