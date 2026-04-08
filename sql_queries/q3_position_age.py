#For each position, find the age group with the highest average potential
#Here, we can use the concept of CTE.
#There's a rule that we should not use the same aliasings in the same Select statement. So, we require separate select statement
#for that aliasing. See the commented query for understanding. It is throwing error.

print("\n-------------------------------")
print("Shuru Project - q3_position_age")
print("-------------------------------")

'''
def highest_avg_potential_sql(spark):
    query = """
    select position_, age_group, avg_potential
    from (
         select position_, age_group, round(avg(potential),2) as avg_potential,
                row_number() over (partition by position_ order by avg(potential) desc) as rowing
         from fifa_players
        group by position_, age_group
         ) as abc
        where rowing = 1
    """
    return spark.sql(query)
'''

#That's why, let's use CTE concept.

def highest_avg_potential_sql(spark):
    query = """
    with agg_alias as (
    select position, age_group, round(avg(potential), 2) as avg_potential
    from fifa_players
    group by position, age_group
    )
    select position, age_group, avg_potential
    from (
         select *, row_number() over (partition by position order by avg_potential desc) as rowing
        from agg_alias
         ) as abc
    where rowing = 1
    """
    return spark.sql(query)