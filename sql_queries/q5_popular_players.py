#Find the Most popular player (by hits) for each position per team
#If we observe, we need to identify the top players for each position and each team based on hits

def popular_players_sql(spark):
    query = """
    with cte as (
    select player_id, name, hits, position, team,
            row_number() over (partition by position, team order by hits desc, player_id asc) as rowing
    from fifa_players
    )
    select player_id, name, hits, position, team
    from cte
    where rowing = 1
    """
    '''query = """
    select player_id, name, hits, position, team
    from fifa_players
    group by hits, position, team
    """'''
    return spark.sql(query)