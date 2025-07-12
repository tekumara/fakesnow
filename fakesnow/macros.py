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

# emulates https://docs.snowflake.com/en/sql-reference/functions/array_construct_compact
# requires transforms.array_construct_compact
ARRAY_CONSTRUCT_COMPACT = Template(
    """
CREATE OR REPLACE MACRO ${catalog}.array_construct_compact(list) AS (
    SELECT ARRAY_AGG(x)::JSON FROM UNNEST(list) AS t(x) WHERE x IS NOT NULL
);
"""
)

FS_TO_TIMESTAMP = Template(
    """
CREATE OR REPLACE MACRO ${catalog}._fs_to_timestamp(val, scale) AS (
    CASE
        WHEN try_cast(val AS BIGINT) IS NOT NULL
            THEN
                CASE
                    WHEN scale = 0 THEN cast(to_timestamp(val::BIGINT) as TIMESTAMP)
                    WHEN scale = 3 THEN cast(to_timestamp(val::BIGINT / 1000) as TIMESTAMP)
                    WHEN scale = 6 THEN cast(to_timestamp(val::BIGINT / 1000000) as TIMESTAMP)
                    WHEN scale = 9 THEN cast(to_timestamp(val::BIGINT / 1000000000) as TIMESTAMP)
                    ELSE NULL
                END
        ELSE CAST(val AS TIMESTAMP)
    END
);
"""
)


def creation_sql(catalog: str) -> str:
    return f"""
        {EQUAL_NULL.substitute(catalog=catalog)};
        {FS_FLATTEN.substitute(catalog=catalog)};
        {ARRAY_CONSTRUCT_COMPACT.substitute(catalog=catalog)};
        {FS_TO_TIMESTAMP.substitute(catalog=catalog)};
    """
