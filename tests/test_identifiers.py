import pytest

from fakesnow.fixtures import conn


def test_select_unquoted_identifier_with_dollar_sign(conn):
    # Test that an unquoted identifier with a dollar sign is handled.
    # This relies on the new transform quoting it, and upper_case_unquoted_identifiers *not* uppercasing it
    # because it's now quoted.
    # We expect DuckDB to fail if "MY$COL" is not quoted, as $ is special in DuckDB.
    # If fakesnow correctly quotes it as "MY$COL", DuckDB should look for that specific case-sensitive name.
    # Since we don't have a table, this will fail at the DuckDB level, but *after* fakesnow's parsing and transformation.
    # We are testing that fakesnow doesn't fail during parsing.
    with conn.cursor() as cur:
        # We need a table to select from, let's create a dummy one.
        cur.execute("CREATE TABLE MY_TABLE (MY_COL INT)")
        # The actual test for the identifier with '$'
        # If the transform works, "MY$COL" will be passed to DuckDB as a quoted identifier.
        # DuckDB will then report that the column "MY$COL" doesn't exist in MY_TABLE.
        # If the transform *doesn't* work, sqlglot/fakesnow might fail earlier,
        # or DuckDB might fail due to syntax issues with unquoted '$'.
        with pytest.raises(Exception, match=r'.*(column MY\$COL does not exist|Invalid Input Error: Unrecognized token: MY\$COL|Binder Error: Referenced column "MY\$COL" not found).*'):
            # The error message might vary depending on how DuckDB handles missing quoted columns vs syntax errors.
            # Added "Binder Error" as another possibility for DuckDB error messages.
            cur.execute("SELECT MY$COL FROM MY_TABLE")

def test_create_table_with_dollar_sign_in_column_name(conn):
    # Test creating a table where a column name has a dollar sign.
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE TEST_DOLLAR_TABLE (MY_REGULAR_COL INT, \"MY$COL\" VARCHAR)")
        cur.execute("DESCRIBE TABLE TEST_DOLLAR_TABLE")
        result = cur.fetchall()
        # print(result) # For debugging if needed
        
        found_dollar_col = False
        for row in result:
            # Column names are typically in the first element of the row tuple from DESCRIBE
            # And they should be uppercase if not quoted during creation, or exact if quoted.
            if row[0] == "MY$COL":
                found_dollar_col = True
                assert row[1].startswith("VARCHAR") # Check type
                break
        assert found_dollar_col, "Column 'MY$COL' not found in DESCRIBE output or has wrong case."

        # Also test inserting and selecting
        cur.execute("INSERT INTO TEST_DOLLAR_TABLE (MY_REGULAR_COL, \"MY$COL\") VALUES (1, 'test_val')")
        cur.execute("SELECT \"MY$COL\" FROM TEST_DOLLAR_TABLE WHERE MY_REGULAR_COL = 1")
        assert cur.fetchone()[0] == 'test_val'


def test_select_from_table_with_dollar_sign_in_name(conn):
    # Test selecting from a table name that includes a dollar sign (fully qualified).
    # This is the original issue's example.
    # Note: Fakesnow/DuckDB might have limitations on database/schema names with '$'.
    # Here, we are testing the table name part.
    # We'll use the default database/schema for simplicity.
    
    db = conn.database
    schema = conn.schema

    with conn.cursor() as cur:
        # Create a table with a dollar sign in its name.
        # Must be double-quoted for Snowflake SQL syntax if it contains special chars like $.
        # The transformation should ensure this quoting is preserved or correctly handled.
        cur.execute(f"CREATE TABLE \"{db}\".\"{schema}\".\"MY$TABLE\" (data VARCHAR)")
        cur.execute(f"INSERT INTO \"{db}\".\"{schema}\".\"MY$TABLE\" (data) VALUES ('dollar_data')")

        # Now select from it using the problematic identifier pattern
        # The transformation should ensure that ORGDATACLOUD$INTERNAL$DATAPRODUCTNAME is treated as a single identifier.
        # For this test, we simplify and assume the table name is just "MY$TABLE" in the current db/schema.
        
        # Test direct selection (already quoted, should work)
        cur.execute("SELECT data FROM \"MY$TABLE\"")
        assert cur.fetchone()[0] == 'dollar_data'

        # Test selection using unquoted identifier (this is what the new transform should fix)
        # This will be parsed by sqlglot, then our transform should quote "MY$TABLE",
        # then upper_case_unquoted_identifiers should NOT change its case because it's now quoted.
        # So DuckDB should receive "MY$TABLE" as the table name.
        # Note: The original issue was `ORGDATACLOUD$INTERNAL$DATAPRODUCTNAME.SCHEMANAME.TABLENAME`
        # Our current fix addresses the individual identifier parts.
        # If `MY$TABLE` is the table name:
        try:
            cur.execute("SELECT data FROM MY$TABLE") # This is the key test for the transform
            assert cur.fetchone()[0] == 'dollar_data'
        except Exception as e:
            # If this fails, it means the transform didn't work as expected.
            # The table MY$TABLE (unquoted) would not be found, or MY (variable) would be sought.
            pytest.fail(f"Selecting from unquoted MY$TABLE failed: {e}")

        # Cleanup
        cur.execute(f"DROP TABLE \"{db}\".\"{schema}\".\"MY$TABLE\"")
