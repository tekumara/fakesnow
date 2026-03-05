from __future__ import annotations

import logging
import os
from collections import OrderedDict
from pathlib import Path
from typing import Any

import duckdb

import fakesnow.fakes as fakes
import fakesnow.macros as macros
from fakesnow import info_schema
from fakesnow.transforms import show

logger = logging.getLogger("fakesnow.instance")

GLOBAL_DATABASE_NAME = "_fs_global"


class FakeSnow:
    def __init__(
        self,
        create_database_on_connect: bool = True,
        create_schema_on_connect: bool = True,
        db_path: str | os.PathLike | None = None,
        nop_regexes: list[str] | None = None,
    ):
        self.create_database_on_connect = create_database_on_connect
        self.create_schema_on_connect = create_schema_on_connect
        self.db_path = db_path
        self.nop_regexes = nop_regexes

        self.results_cache: OrderedDict[str, tuple] = OrderedDict()
        self.duck_conn = duckdb.connect(database=":memory:")

        # create a "global" database for storing objects which span databases.
        self.duck_conn.execute(f"ATTACH IF NOT EXISTS ':memory:' AS {GLOBAL_DATABASE_NAME}")
        # create the info schema extensions and show views
        self.duck_conn.execute(info_schema.fs_global_creation_sql())
        self.duck_conn.execute(show.fs_global_creation_sql())

        # use UTC instead of local time zone for consistent testing
        self.duck_conn.execute("SET GLOBAL TimeZone = 'UTC'")

        # Attach existing database files from db_path for persistence across restarts
        if self.db_path:
            self._attach_existing_databases()

    def _attach_existing_databases(self) -> None:
        """Scan db_path for existing .db files and attach them."""
        db_path = Path(self.db_path)  # type: ignore[arg-type]
        if not db_path.is_dir():
            logger.warning(f"db_path does not exist or is not a directory: {db_path}")
            return

        for db_file in db_path.glob("*.db"):
            # Database name is the filename without .db extension (uppercase for Snowflake convention)
            db_name = db_file.stem.upper()

            # Skip if already attached
            if self.duck_conn.execute(
                f"SELECT * FROM information_schema.schemata WHERE upper(catalog_name) = ?",
                parameters=(db_name,)
            ).fetchone():
                logger.info(f"Database {db_name} already attached, skipping")
                continue

            logger.info(f"Attaching existing database: {db_name} from {db_file}")
            self.duck_conn.execute(f"ATTACH DATABASE ? AS ?", parameters=(str(db_file), db_name))
            self.duck_conn.execute(info_schema.per_db_creation_sql(db_name))
            self.duck_conn.execute(macros.creation_sql(db_name))

    def connect(
        self,
        database: str | None = None,
        schema: str | None = None,
        nop_regexes: list[str] | None = None,
        **kwargs: Any,
    ) -> fakes.FakeSnowflakeConnection:
        # every time we connect, create a new cursor (ie: connection) so we can isolate each connection's
        # schema setting see
        # https://github.com/duckdb/duckdb/blob/18254ec/tools/pythonpkg/src/pyconnection.cpp#L1440
        # and to make connections thread-safe see
        # https://duckdb.org/docs/api/python/overview.html#using-connections-in-parallel-python-programs
        return fakes.FakeSnowflakeConnection(
            self.duck_conn.cursor(),
            self.results_cache,
            database,
            schema,
            create_database=self.create_database_on_connect,
            create_schema=self.create_schema_on_connect,
            db_path=self.db_path,
            nop_regexes=nop_regexes or self.nop_regexes,
            **kwargs,
        )
