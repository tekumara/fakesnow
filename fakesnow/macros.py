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
        '[' || (idx - 1) || ']' AS PATH,
        idx - 1 AS INDEX,
        value AS VALUE,
        TO_JSON(input) AS THIS
    FROM UNNEST(CAST(TO_JSON(input) AS JSON [])) WITH ORDINALITY AS t(value, idx)
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

# use json_group_object instead of json_object because it filters out keys that are null
# see https://github.com/duckdb/duckdb/issues/19357
FS_OBJECT_CONSTRUCT = Template(
    """
CREATE OR REPLACE MACRO ${catalog}._fs_object_construct(keys, vals, keep_nulls) AS (
    WITH kv AS (
        SELECT
            key,
            list_extract(vals, idx) AS value
        FROM UNNEST(keys) WITH ORDINALITY AS u(key, idx)
        ORDER BY idx
    )
    SELECT json_group_object(key, value) AS obj
    FROM kv
    WHERE keep_nulls OR value IS NOT NULL
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
        {FS_OBJECT_CONSTRUCT.substitute(catalog=catalog)};
        {FS_TO_TIMESTAMP.substitute(catalog=catalog)};
    """
