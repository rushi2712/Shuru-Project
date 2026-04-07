#We need to identify the top 5 players with the highest growth_potential in each team
print("\n------------------------------")
print("Shuru Project - q1_top_players")
print("------------------------------")
#print("Query 1: The top 5 players from each team:")

def top_5_players_sql(spark):
    query = """
    select name, team, growth_potential
    from (
         select name, team, growth_potential,
                row_number() over (partition by team order by growth_potential desc) as rowing
         from fifa_players
         ) as q1
    where rowing <= 5
    """
    return spark.sql(query)