#Find the top 10 nationalities by average overall of their players

def top_10_natl_avgovrl_sql(spark):
    query = """
    select nationality, round(avg(overall), 2) as avg_overall
    from fifa_players
    group by nationality
    order by avg_overall desc
    limit 10
    """

    return spark.sql(query)