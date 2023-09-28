from parsons.databases.database.database import DatabaseCreateStatement
import parsons.databases.sqlite.constants as consts

import petl
import logging

logger = logging.getLogger(__name__)


class SQLiteCreateTable(DatabaseCreateStatement):
    def __init__(self):
        super().__init__()

    # This is for backwards compatability
    def data_type(self, val, current_type):
        return self.detect_data_type(val, current_type)

    # This is for backwards compatability
    def is_valid_integer(self, val):
        return self.is_valid_sql_num(val)

    # I'm unsure how much of this is actually relevant to the way SQLite is constructed
    # SQLite datatypes are very simplistic compared to other SQL variants -- see
    # datatype comparisons below
    # https://www.sqlite.org/datatype3.html
    def evaluate_column(self, column_rows):
        # Generate MySQL data types and widths for a column.

        col_type = None

        # Iterate through each row in the column
        for row in column_rows:

            # Get the MySQL data type
            col_type = self.data_type(row, col_type)

        return col_type

    def evaluate_table(self, tbl):
        # Generate a dict of MySQL column types and widths for all columns
        # in a table.

        table_map = []

        for col in tbl.columns:
            col_type = self.evaluate_column(tbl.column_data(col))
            col_map = {"name": col, "type": col_type}
            table_map.append(col_map)

        return table_map

    def create_statement(self, tbl, table_name):
        # Generate create statement SQL for a given Parsons table.

        # Validate and rename column names if needed
        tbl.table = petl.setheader(tbl.table, self.columns_convert(tbl.columns))

        # Generate the table map
        table_map = self.evaluate_table(tbl)
        print(table_map)

        # Generate the column syntax
        column_syntax = []
        for c in table_map:
            column_syntax.append(f"{c['name']} {c['type']} \n")

        # Generate full statement
        return f"CREATE TABLE {table_name} ( \n {','.join(column_syntax)});"

    
    # This is for backwards compatibility
    def columns_convert(self, columns):
        return self.format_columns(columns, col_prefix="col_")

    