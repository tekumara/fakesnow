from string import Template

EQUAL_NULL = Template(
    """
CREATE MACRO IF NOT EXISTS ${catalog}.equal_null(a, b) AS a IS NOT DISTINCT FROM b;
"""
)

# emulate the Snowflake FLATTEN function for ARRAYs
# see https://docs.snowflake.com/en/sql-reference/functions/flatten.html
FS_FLATTEN = Template(
    """
CREATE OR REPLACE MACRO ${catalog}._fs_flatten(input) AS TABLE
    SELECT
        NULL AS SEQ, -- TODO use a sequence and nextval
        CAST(NULL AS VARCHAR) AS KEY,
        '[' || GENERATE_SUBSCRIPTS(
            CAST(TO_JSON(input) AS JSON []),
            1
        ) - 1 || ']' AS PATH,
        GENERATE_SUBSCRIPTS(
            CAST(TO_JSON(input) AS JSON []),
            1
        ) - 1 AS INDEX,
        UNNEST(
            CAST(TO_JSON(input) AS JSON [])
        ) AS VALUE,
        TO_JSON(input) AS THIS;
    """
)


def creation_sql(catalog: str) -> str:
    return f"""
        {EQUAL_NULL.substitute(catalog=catalog)};
        {FS_FLATTEN.substitute(catalog=catalog)};
    """
