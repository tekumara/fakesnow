from string import Template

# emulate the Snowflake FLATTEN function for ARRAYs and OBJECTTs
# see https://docs.snowflake.com/en/sql-reference/functions/flatten.html
FS_FLATTEN = Template(
    """
CREATE OR REPLACE MACRO ${catalog}._fs_flatten(input) AS TABLE
    SELECT
        -- SEQ: hash of input gives same value for all rows from same input, close enough to Snowflake's SEQ
        hash(TO_JSON(input))::UBIGINT AS SEQ,
        e.k AS KEY,
        COALESCE(e.k, '[' || (row_number() OVER () - 1) || ']') AS PATH,
        CASE WHEN e.k IS NOT NULL THEN NULL ELSE (row_number() OVER () - 1)::BIGINT END AS INDEX,
        e.v AS VALUE,
        TO_JSON(input) AS THIS
    FROM (
        SELECT UNNEST(
            CASE WHEN json_type(TO_JSON(input)) = 'OBJECT'
                 THEN list_transform(
                    json_keys(TO_JSON(input)),
                    x -> struct_pack(k := x, v := CAST(TO_JSON(input) -> x AS JSON))
                 )
                 ELSE list_transform(
                    CAST(TO_JSON(input) AS JSON[]),
                    x -> struct_pack(k := NULL::VARCHAR, v := x)
                 )
            END, recursive := true
        )
    ) AS e(k, v)
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
        {FS_FLATTEN.substitute(catalog=catalog)};
        {FS_OBJECT_CONSTRUCT.substitute(catalog=catalog)};
        {FS_TO_TIMESTAMP.substitute(catalog=catalog)};
    """
