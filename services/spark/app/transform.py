from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, array, lit, expr

TECH_KEYWORDS = ["python","sql","spark","kafka","aws","docker","react","typescript"]
ROLE_RULES = ["backend engineer", "frontend engineer", "data scientist", "software engineer"]

def add_role_category(df: DataFrame) -> DataFrame:
    expr = None

    for role in ROLE_RULES:
        condition = col("title_l").contains(role) # add a contains() for each role onto expr
        if expr is None:
            expr = when(condition, lit(role.title())) # start expression
        else:
            expr = expr.when(condition, lit(role.title())) # add .when() onto expression

    expr = expr.otherwise(lit("Other")) # else case, if none match use "Other" role

    return df.withColumn("role_category", expr)

def add_techs(df: DataFrame) -> DataFrame:
    # build list of matched techs
    tech_cols = []

    for tech in TECH_KEYWORDS:
        tech_cols.append(
            # if description or title contain keyword, add it as a column ( lit(tech) )
            when(col("desc_l").contains(tech) | col("title_l").contains(tech), lit(tech))
        )

    # create array (with nulls)
    df = df.withColumn("techs_raw", array(*tech_cols))

    # remove nulls from array
    df = df.withColumn("techs", expr("filter(techs_raw, x -> x is not null)"))

    return df