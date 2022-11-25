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

    def copy_table(self, from_table, to_table, id_name,
                   from_schema=None, to_schema=None,
                   where_col=None, equals_to=None):
        """
        Copies the table in an append-like fashion

        :param from_table:
        :param to_table:
        :param id_name:  archivation id column name
        :param from_schema:
        :param to_schema:
        :param where_col:
        :param equals_to:
        :return:
        """

        if from_schema is None: from_schema = self.in_schemas[self.task_type]["main_schema"]["schema_name"]
        if to_schema is None: to_schema = self.in_schemas[self.task_type]["archive_schema"]["schema_name"]
        from_columns = self._get_intersecting_columns(from_schema, from_table, to_schema, to_table)
        _id = self.conn.get_max_col_number(to_table, to_schema, id_name)

        if equals_to is None:
            self.conn.copy_table(from_table, to_table, from_schema, to_schema,
                                 from_columns, _id + 1, id_name)
        else:
            self.conn.copy_table_where(from_table, to_table, from_schema, to_schema,
                                       from_columns, _id + 1, id_name, where_col, equals_to)

    def _get_intersecting_columns(self, from_schema, from_table, to_schema, to_table):
        from_columns = self.conn.get_column_names(from_table, from_schema)
        to_columns = self.conn.get_column_names(to_table, to_schema)
        intersect_cols = list(set(from_columns) & set(to_columns))
        formatted_cols = ", ".join(intersect_cols)
        return formatted_cols

    def copy_tables(self, from_tables, to_tables, id_archive_name, where_cols, equal_to_values):
        """
        Аналог функции copy_table, однако используемый для нескольких таблиц. from_tables, to_tables,
        where_cols, equal_to_values должны быть переданы как list обязательно.

        :param from_tables:
        :param to_tables:
        :param where_cols: list. Колонки, по которым проходит фильтрация,
        :param equal_to_values: list. Значения на которые проверяются экспортируемые ряды.
        :return:
        """
        self._check_for_length(from_tables, to_tables)
        equal_to_values, where_cols = self._normalise_filter_values(equal_to_values, where_cols, len(from_tables))

        for from_table, to_table, where_col, equals_to_ \
                in zip(from_tables, to_tables, where_cols, equal_to_values):
            self.copy_table(from_table, to_table, id_archive_name,
                            where_col=where_col, equals_to=equals_to_)

    def _normalise_filter_values(self, equal_to_values, where_cols, elements_num):
        if len(equal_to_values) == len(where_cols) == 1:
            self._check_for_filter_types(equal_to_values, where_cols)
            equal_to_values = equal_to_values * elements_num
            where_cols = where_cols * elements_num
        return equal_to_values, where_cols

    def _check_for_filter_types(self, equal_to_values, where_cols):
        if type(equal_to_values) != list != type(where_cols):
            raise TypeError(
                f'{self.copy_tables.__name__}: параметры equal_to_values и where_cols обязаны быть list!')

    def _check_for_length(self, from_tables, to_tables):
        if len(from_tables) != len(to_tables):
            raise IndexError(
                f'{self.copy_tables.__name__}: trying to pass different number of from_tables and to_tables')

    def get_json_table_names(self, schema, without=None):
        tables = self.config[schema]["tables"]
        if without is not None: without_name = tables[without]["table_name"]
        else: without_name = None

        names = [v["table_name"] for k, v in tables.items() if v["table_name"] != without_name]
        return names

