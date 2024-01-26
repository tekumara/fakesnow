from string import Template

EQUAL_NULL = Template(
    """
CREATE MACRO IF NOT EXISTS ${catalog}.equal_null(a, b) AS a IS NOT DISTINCT FROM b;
"""
)


def creation_sql(catalog: str) -> str:
    return f"""
        {EQUAL_NULL.substitute(catalog=catalog)};
    """
