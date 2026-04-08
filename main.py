print("\n--------------------")
print("Shuru Project - MAIN")
print("--------------------")

#This is the main file which executes all the operations from here
from ingestion import read_player_data
from data_cleaning import fifa_clean
from transformation import fifa_trans
from sql_queries.q1_top_players import top_5_players_sql
from sql_queries.q2_best_teams import highest_average_overall_23_sql
from sql_queries.q3_position_age import highest_avg_potential_sql
from sql_queries.q4_nationality import top_10_natl_avgovrl_sql
from sql_queries.q5_popular_players import popular_players_sql
from storage import store_transformed_data, store_query_output

#Step 1: INGESTION
fifa_read, spark= read_player_data()

#Step 2: DATA_CLEANING
fifa_cleaned = fifa_clean(fifa_read)

#Step 3: TRANSFORMATION
fifa_transformed = fifa_trans(fifa_cleaned)

#Step 4.1: Create temp view for Query. 1 TempView is sufficient as we have only table
fifa_transformed.createOrReplaceTempView("fifa_players")
#Note: We can use the same TempView for the rest of the queries
#Step 4.1.1: Query 1 - Top 5 players from each team
top_5_players = top_5_players_sql(spark)
print("Query 1: The top 5 players from each team based on growth_potential:")
top_5_players.show()

#Step 4.1.2: Query 2 - Highest team average among players under 23 age
highest_average_overall_23 = highest_average_overall_23_sql(spark)
print("Query 2: The highest team average among players under 23 age")
highest_average_overall_23.show()

#Step 4.1.3: Query 3 - For each position, find the age group with the highest average potential
highest_avg_potential = highest_avg_potential_sql(spark)
print("Query 3: For each position, the highest age group with highest average potential:")
highest_avg_potential.show()

#Step 4.1.4: Query 4 - To find the top 10 nationalities by average overall of their players
top_10_natl_avgovrl = top_10_natl_avgovrl_sql(spark)
print("Query 4: Finding the top 10 nationalities based on average of overall of the players")
top_10_natl_avgovrl.show()

#Step 4.1.5: Query 5 - To find the top 10 nationalities by average overall of their players
popular_players = popular_players_sql(spark)
print("Query 5: Finding the Most popular player (by hits) for each position per team")
popular_players.show()

#Step 5: STORAGE of Transformed data
store_transformed_data(fifa_transformed)
#Step 6: Storage of Query data
store_query_output(df = top_5_players, folder_name = "q1_top_players")
store_query_output(df = highest_average_overall_23, folder_name = "q2_best_teams")
store_query_output(df = highest_avg_potential, folder_name = "q3_position_age")
store_query_output(df = highest_avg_potential, folder_name = "q4_nationality")
store_query_output(df = popular_players, folder_name = 'q5_popular_players')