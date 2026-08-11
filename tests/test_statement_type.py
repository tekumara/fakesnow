import pytest
import sqlglot
from sqlglot import Expr

import fakesnow.statement_type as statement_type


def parse(s: str) -> Expr:
    return sqlglot.parse_one(s, read="snowflake")


def type_id(s: str) -> int:
    return statement_type.statement_type_id(parse(s))


def test_statement_type_id_select() -> None:
    assert type_id("select * from customers") == statement_type.SELECT
    assert type_id("with c as (select 1) select * from c") == statement_type.SELECT


def test_statement_type_id_dml() -> None:
    assert type_id("insert into customers values (1)") == statement_type.INSERT
    assert type_id("update customers set id = 1") == statement_type.UPDATE
    assert type_id("delete from customers") == statement_type.DELETE
    assert type_id("merge into t1 using t2 on t1.id = t2.id when matched then update set t1.id = 1") == (
        statement_type.MERGE
    )
    assert type_id("insert all into t1 values (1) into t2 values (2) select 1") == statement_type.MULTI_INSERT
    assert type_id("copy into customers from @stage1") == statement_type.COPY


def test_statement_type_id_ddl() -> None:
    assert type_id("create table customers (id int)") == statement_type.DDL
    assert type_id("create or replace view v as select 1") == statement_type.DDL
    assert type_id("create schema foobar") == statement_type.DDL
    assert type_id("drop table customers") == statement_type.DDL
    assert type_id("alter table customers add column name varchar") == statement_type.DDL
    assert type_id("truncate table customers") == statement_type.DDL


@pytest.mark.parametrize(
    "sql",
    [
        "use role foobar",
        "use database foobar",
        "use schema foobar",
        "use warehouse foobar",
    ],
)
def test_statement_type_id_use(sql: str) -> None:
    assert type_id(sql) == statement_type.USE


def test_statement_type_id_scl() -> None:
    assert type_id("show tables") == statement_type.SHOW
    assert type_id("describe table customers") == statement_type.DESCRIBE


def test_statement_type_id_tcl() -> None:
    assert type_id("begin") == statement_type.TCL
    assert type_id("commit") == statement_type.TCL
    assert type_id("rollback") == statement_type.TCL


def test_statement_type_id_unknown() -> None:
    assert type_id("put file:///tmp/customers.csv @stage1") == statement_type.UNKNOWN
