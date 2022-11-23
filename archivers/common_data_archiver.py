import warnings
import pandas as pd
from pandas.errors import SettingWithCopyWarning


class CommonDataArchiver:
    def __init__(self, conn, in_schemas, logger, task_type):
        self.conn = conn
        self.in_schemas = in_schemas
        self.logger = logger  # unused
        self.task_type = task_type
        self.config = in_schemas[task_type]


    def get_table_name(self, table):
        return self.in_schemas[table]["table_name"]

    def copy_table(self, from_schema, from_table, to_schema, to_table, id_name):
        """
        Copies the table in an append-like fashion

        :param from_table:
        :param to_table:
        :param from_schema:
        :param to_schema:
        :param id_name: archivation id column name
        :return:
        """
        if from_schema is None: from_schema = self.in_schemas[self.task_type]["main_schema"]["schema_name"]
        if to_schema is None: to_schema = self.in_schemas[self.task_type]["archive_schema"]["schema_name"]
        from_columns = ", ".join(self.conn.get_column_names(from_table, from_schema))
        _id = self.conn.get_max_col_number(to_table, to_schema, id_name)
        self.conn.copy_table(from_table, to_table, from_schema, to_schema, from_columns, _id + 1, id_name)




