# We need to find the teams with the highest average overall among players under 23
print("\n-----------------------------")
print("Shuru Project - q2_best_teams")
print("-----------------------------")
#print("Query 2: The highest team average among players under 23 age")

def highest_average_overall_23_sql(spark):
    query = """
    select team, round(avg(overall), 2) as avg_overall
    from fifa_players
    where age <= 23
    group by team
    order by avg_overall desc
    """
    return spark.sql(query)