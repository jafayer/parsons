#stdlib
import logging
import os
from contextlib import contextmanager
import sqlite3
import pickle

# dependencies
import petl

# parsons dependencies
from parsons import Table
from parsons.utilities import check_env
from parsons.utilities import files
from parsons.databases.database_connector import DatabaseConnector
from parsons.databases.table import BaseTable
from parsons.databases.sqlite.sqlite_create_table import SQLiteCreateTable
from parsons.databases.alchemy import Alchemy


QUERY_BATCH_SIZE = 100000

logger = logging.getLogger(__name__)

"""
TODO: explore whether it's better to persist the connection for an in-memory database or create a tempfile as we're doing now.
In order to conform to the conventional database connector structure,
The connection for this database is closed after every query due to the use of python contexts
This means that any in-memory database is deleted as soon as the transaction closes, making it close to useless
for any database that should be in-memory.

It's possible we want both! Either the ability to instantiate the db as a tempfile to support throwaway
databases that are nevertheless durable during the lifespan of the script, and that can accommodate more data
because they're not dependent on RAM
OR, for use cases where it's undesirable to write to disk, the ability to create a truly in-memory database.
Unclear on the best implementation there, though
"""
class SQLite(DatabaseConnector, SQLiteCreateTable, Alchemy):
    """
    Connect to a MySQL database.

    `Args:`
        file: str
            Optional path to file for SQLite database. If no path is provided, a temp file is created.
    """

    def __init__(self, file=files.create_temp_file()):
        super().__init__()

        self.file = file

    @contextmanager
    def connection(self):
        connection = sqlite3.connect(self.file)

        try:
            yield connection
        except sqlite3.Error:
            connection.rollback()
        finally:
            print("Closed connection")
            connection.close()
    

    @contextmanager
    def cursor(self, connection):
        cursor = connection.cursor()

        try:
            yield cursor
        finally:
            print("Closed cursor")
            cursor.close()

    def query(self, sql, parameters=None):
        """
        Execute a query against the database. Will return ``None`` if the query returns zero rows.

        To include python variables in your query, it is recommended to pass them as parameters,
        following the `mysql style <https://security.openstack.org/guidelines/dg_parameterize-database-queries.html>`_.
        Using the ``parameters`` argument ensures that values are escaped properly, and avoids SQL
        injection attacks.

        **Parameter Examples**

        .. code-block:: python

            # Note that the name contains a quote, which could break your query if not escaped
            # properly.
            name = "Beatrice O'Brady"
            sql = "SELECT * FROM my_table WHERE name = %s"
            mysql.query(sql, parameters=[name])

        .. code-block:: python

            names = ["Allen Smith", "Beatrice O'Brady", "Cathy Thompson"]
            placeholders = ', '.join('%s' for item in names)
            sql = f"SELECT * FROM my_table WHERE name IN ({placeholders})"
            mysql.query(sql, parameters=names)

        `Args:`
            sql: str
                A valid SQL statement
            parameters: list
                A list of python variables to be converted into SQL values in your query

        `Returns:`
            Parsons Table
                See :ref:`parsons-table` for output options.

        """  # noqa: E501

        with self.connection() as connection:
            return self.query_with_connection(sql, connection, parameters=parameters)

    def query_with_connection(self, sql, connection, parameters=None, commit=True):
        """
        Execute a query against the database, with an existing connection. Useful for batching
        queries together. Will return ``None`` if the query returns zero rows.

        `Args:`
            sql: str
                A valid SQL statement
            connection: obj
                A connection object obtained from ``mysql.connection()``
            parameters: list
                A list of python variables to be converted into SQL values in your query
            commit: boolean
                Whether to commit the transaction immediately. If ``False`` the transaction will
                be committed when the connection goes out of scope and is closed (or you can
                commit manually with ``connection.commit()``).

        `Returns:`
            Parsons Table
                See :ref:`parsons-table` for output options.
        """
        with self.cursor(connection) as cursor:
            # The python connector can only execute a single sql statement, so we will
            # break up each statement and execute them separately.
            for s in sql.strip().split(";"):
                if len(s) != 0:
                    logger.debug(f"SQL Query: {sql}")
                    t = cursor.execute(s)
                    connection.commit()
                    
                    print(t)

            # If the SQL query provides no response, then return None
            if not cursor.description:
                logger.debug("Query returned 0 rows")
                return None

            else:
                # Fetch the data in batches, and "pickle" the rows to a temp file.
                # (We pickle rather than writing to, say, a CSV, so that we maintain
                # all the type information for each field.)

                temp_file = files.create_temp_file()

                with open(temp_file, "wb") as f:
                    # Grab the header
                    print("Description", [d[0] for d in cursor.description])
                    pickle.dump([d[0] for d in cursor.description], f)

                    while True:
                        batch = cursor.fetchmany(QUERY_BATCH_SIZE)
                        print(batch)
                        if len(batch) == 0:
                            break

                        logger.debug(f"Fetched {len(batch)} rows.")
                        for row in batch:
                            pickle.dump(row, f)

                # Load a Table from the file
                final_tbl = Table(petl.frompickle(temp_file))

                logger.debug(f"Query returned {final_tbl.num_rows} rows.")
                return final_tbl

    def copy(
        self,
        tbl: Table,
        table_name: str,
        if_exists: str = "fail",
        chunk_size: int = 1000,
    ):
        """
        Copy a :ref:`parsons-table` to the database.

        .. note::
            This method utilizes extended inserts rather `LOAD DATA INFILE` since
            many MySQL Database configurations do not allow data files to be
            loaded. It results in a minor performance hit compared to `LOAD DATA`.

        `Args:`
            tbl: parsons.Table
                A Parsons table object
            table_name: str
                The destination schema and table (e.g. ``my_schema.my_table``)
            if_exists: str
                If the table already exists, either ``fail``, ``append``, ``drop``
                or ``truncate`` the table.
            chunk_size: int
                The number of rows to insert per query.
        """

        if tbl.num_rows == 0:
            logger.info("Parsons table is empty. Table will not be created.")
            return None

        with self.connection() as connection:
            # Create table if not exists
            if self._create_table_precheck(connection, table_name, if_exists):
                sql = self.create_statement(
                    tbl, table_name
                )
                self.query_with_connection(sql, connection)
                logger.info(f"Table {table_name} created.")

            # Chunk tables in batches of 1K rows, though this can be tuned and
            # optimized further.
            for t in tbl.chunk(chunk_size):
                sql = self._insert_statement(t, table_name)
                self.query_with_connection(sql, connection)

    def _insert_statement(self, tbl, table_name):
        """
        Convert the table data into a string for bulk importing.
        """

        # Single column tables
        if len(tbl.columns) == 1:
            values = [f"({row[0]})" for row in tbl.data]

        # Multi-column tables
        else:
            values = [str(row) for row in tbl.data]

        # Create full insert statement
        sql = f"""INSERT INTO {table_name}
                  ({','.join(tbl.columns)})
                  VALUES {",".join(values)};"""

        return sql

    def _create_table_precheck(self, connection, table_name, if_exists):
        """
        Helper to determine what to do when you need a table that may already exist.

        `Args:`
            connection: obj
                A connection object obtained from ``mysql.connection()``
            table_name: str
                The table to check
            if_exists: str
                If the table already exists, either ``fail``, ``append``, ``drop``,
                or ``truncate`` the table.
        `Returns:`
            bool
                True if the table needs to be created, False otherwise.
        """

        if if_exists not in ["fail", "append", "drop"]:
            raise ValueError("Invalid value for `if_exists` argument")

        # If the table exists, evaluate the if_exists argument for next steps.
        if self.table_exists(table_name):
            if if_exists == "fail":
                raise ValueError("Table already exists.")

            if if_exists == "drop":
                sql = f"DROP TABLE {table_name}"
                self.query_with_connection(sql, connection)
                logger.info(f"{table_name} dropped.")
                return True

        else:
            return True

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table or view exists in the database.

        `Args:`
            table_name: str
                The table name

        `Returns:`
            boolean
                ``True`` if the table exists and ``False`` if it does not.
        """

        if self.query(f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{table_name}'").first == table_name:
            return True
        else:
            return False

    def table(self, table_name):
        # Return a BaseTable table object

        return BaseTable(self, table_name)