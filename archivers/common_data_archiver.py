from psycopg2._psycopg import AsIs
import traceback


class DataArchiver:
    def __init__(self, conn, in_schemas, logger, task_type):
        self.meta_dataset_id = None
        self.conn = conn
        self.logger = logger
        self.task_type = task_type
        self.config = in_schemas[task_type]

    def run(self, d: dict):
        task_type = d['file_type']
        mode = d["type"]
        self.meta_dataset_id = d["metaload_dataset_id"]
        self.logger.info(f"{task_type} running")

        try:
            if mode == "archive":
                self.archive_tables()
            elif mode == "recover":
                self.recover_tables_from_archive()
            else:
                raise ValueError(
                    "Значение mode должно равняться 'archive' или 'recover', проверьте правильность поля 'type' в "
                    "приходящем JSON")

            self.conn.conn.commit()
            status = True
        except Exception as e:
            self.logger.error(f"[НЕОБРАБОТАННАЯ ОШИБКА] При архивации {task_type} возникла неизвестная ошибка:\n"
                              f"{e}!\n\n Откат изменений")
            traceback.print_exc()
            traceback.format_exc()
            self.conn.conn.rollback()
            status = False
        return status

    def prepare_copying_tables(self, from_schema, to_schema):
        from_tables = self._get_db_table_names(without="upload_files")
        to_tables = self._get_db_table_names(without="upload_files")

        from_tables_json_names = self._get_json_table_names(without="upload_files")

        metaload_id_cols = self._get_required_columns_names_for_bd(json_column_name="metaload_dataset_id",
                                                                   schema=from_schema,
                                                                   tables_json_names=from_tables_json_names)

        self.copy_tables(from_tables=from_tables, to_tables=to_tables,
                         from_schema_type=from_schema, to_schema_type=to_schema,
                         where_cols=metaload_id_cols,
                         equal_to_values=[self.meta_dataset_id])

    def prepare_deleting_tables(self, schema):
        tables_db_names = self._get_db_table_names(without="upload_files")
        tables_json_names = self._get_json_table_names(without="upload_files")

        metaload_id_cols = self._get_required_columns_names_for_bd(json_column_name="metaload_dataset_id",
                                                                   schema=schema,
                                                                   tables_json_names=tables_json_names)

        self.delete_tables(tables_db_names, schema, where_cols=metaload_id_cols, equal_to_values=[self.meta_dataset_id])

    def archive_tables(self):
        main = "main_schema"
        archive = "archive_schema"

        self.copy_metadata_entry(from_schema=main, to_schema=archive)
        self.prepare_copying_tables(from_schema=main, to_schema=archive)
        self.prepare_deleting_tables(schema=main)
        self.delete_metadata_entry(schema=main)

    def recover_tables_from_archive(self):
        archive = "archive_schema"
        main = "main_schema"

        self.copy_metadata_entry(from_schema=archive, to_schema=main)
        self.prepare_copying_tables(from_schema=archive, to_schema=main)
        self.prepare_deleting_tables(schema=archive)
        self.delete_metadata_entry(schema=archive)

    def copy_table(self, from_schema_type: str, to_schema_type: str,
                   from_table: str, to_table: str,
                   where_col=None, equals_to=None):
        """
        Copies the table in an append-like fashion

        :param from_schema_type: Схема, откуда будут скопированы значения
        :param to_schema_type: Схема, куда будет скопированы значеиня
        :param from_table: Из какой таблицы скопировать,
        :param to_table: В какую таблицу скопировать,
        :param where_col: Колонка, по которой проходит фильтрация,
        :param equals_to: Фильтр по колонке where_col.
        :return:
        """
        assert from_schema_type in ("main_schema", "archive_schema")
        assert to_schema_type in ("main_schema", "archive_schema")

        from_schema_name = self.config["schemas"][from_schema_type]
        to_schema_name = self.config["schemas"][to_schema_type]

        from_columns = self._get_intersecting_columns(from_schema_name, from_table, to_schema_name, to_table)
        from_schema_table = self._sql_name(from_schema_name, from_table)
        to_schema_table = self._sql_name(to_schema_name, to_table)

        if equals_to is None:
            data = self.conn.copy_table(from_schema_table, to_schema_table, from_columns)
        else:
            data = self.conn.copy_table_where(from_schema_table, to_schema_table,
                                              from_columns, where_col, equals_to)
            specification = f"ГДЕ '{where_col}' == {equals_to}"

        self._report_results(data, specification=specification, mode="copy",
                             from_table=from_schema_table, to_table=to_schema_table)

    def copy_tables(self, from_schema_type: str, to_schema_type: str,
                    from_tables: list[str], to_tables: list[str],
                    where_cols: list[str], equal_to_values: list[object]):
        """
        Аналог функции copy_table, однако используемый для нескольких таблиц. from_tables, to_tables,
        where_cols, equal_to_values должны быть переданы как list обязательно.

        :param from_schema_type: Схема, откуда будут скопированы значения
        :param to_schema_type: Схема, куда будет скопированы значеиня
        :param from_tables: Из каких таблиц скопировать, порядок должен обязательно совпадать с to_tables, where_cols, equal_to_values
        :param to_tables: В какие таблицы скопировать, порядок должен обязательно совпадать с from_tables, where_cols, equal_to_values
        :param where_cols: Колонки, по которым проходит фильтрация, порядок должен обязательно совпадать с from_tables, to_tables, equal_to_values
        :param equal_to_values: list. Значения-фильтры для колонок where_cols. порядок должен обязательно совпадать с from_tables, to_tables, where_cols
        :return:
        """
        self._check_types_validity(equal_to_values, from_tables, to_tables, where_cols)
        equal_to_values, where_cols = self._normalise_filter_values(equal_to_values, where_cols, len(from_tables))

        for from_table, to_table, where_col, equals_to \
                in zip(from_tables, to_tables, where_cols, equal_to_values):
            self.copy_table(from_table=from_table, to_table=to_table,
                            from_schema_type=from_schema_type, to_schema_type=to_schema_type,
                            where_col=where_col, equals_to=equals_to)

    def _check_types_validity(self, equal_to_values, from_tables, to_tables, where_cols):
        assert len(from_tables) == len(to_tables), f'{self.copy_tables.__name__}:' \
                                                   f' trying to pass different number of from_tables and to_tables'
        assert type(from_tables) == type(to_tables) == type(where_cols) == type(
            equal_to_values) == list, "Parameter has to be a list"

    def delete_table(self, table_name: str, where_col: str, equal_to: object, schema_type: str):
        """
        Удаляет данные из таблицы с заданным фильтром равенства по колонке.
        :param table_name: Имя таблицы, как в бд
        :param where_col: Колонка для фильтрации как в бд
        :param equal_to: Ряды, где колонка where_col равна equal_to будут удалены
        :param schema_type: паблик или архив
        :return:
        """
        assert schema_type in ("main_schema", "archive_schema")
        schema_name = self.config["schemas"][schema_type]

        schema_table_name = self._sql_name(schema_name, table_name)

        data = self.conn.delete_table_where(schema_table_name, where_col, equal_to)
        spec = f"ГДЕ '{where_col}' == {equal_to}"
        self._report_results(data, mode="delete", specification=spec, from_table=schema_table_name)

    def delete_tables(self, tables: list[str], schema: str, where_cols: list[str], equal_to_values: list[object]):
        self._check_types_validity(equal_to_values, tables, tables, where_cols)
        equal_to_values, where_cols = self._normalise_filter_values(equal_to_values, where_cols, len(tables))

        for table, where_col, equal_to in zip(tables, where_cols, equal_to_values):
            self.delete_table(table_name=table, where_col=where_col, equal_to=equal_to, schema_type=schema)

    def copy_metadata_entry(self, from_schema, to_schema):
        """
        copies metadata_table entry from public_schema to archive_schema
        :return:
        """
        upload_files = self.config["tables"]["upload_files"]
        from_table = to_table = upload_files["table_name"]

        meta_dataset_col = upload_files["req_cols"]["metaload_dataset_id"]

        self.copy_table(from_table=from_table, to_table=to_table,
                        where_col=meta_dataset_col, equals_to=self.meta_dataset_id,
                        from_schema_type=from_schema, to_schema_type=to_schema)

    def delete_metadata_entry(self, schema):
        """
        deletes metadata_table entry from public_schema
        :return:
        """
        upload_files_table = self.config["tables"]["upload_files"]
        table = upload_files_table["table_name"]
        meta_dataset_col = upload_files_table["req_cols"]["metaload_dataset_id"]
        self.delete_table(table_name=table, schema_type=schema, where_col=meta_dataset_col,
                          equal_to=self.meta_dataset_id)

    def _report_results(self, data, specification, mode=None, from_table=None, to_table=None):
        """
        :param data:
        :param specification:
        :param mode:
        :param from_table:
        :param to_table:
        :return:
        """
        match mode:
            case "copy":
                if self._query_result_empty(data):
                    self.logger.warning(
                        f"Из таблицы '{from_table}' не было найдено ни одного ряда {specification}. Ничего не было "
                        f"скопировано!")
                else:
                    self.logger.info(
                        f"Таблица '{from_table}' скопирована в таблицу '{to_table}' {specification}")
            case "delete":
                if self._query_result_empty(data):
                    self.logger.warning(
                        f"Из таблицы '{from_table}' не было найдено ни одного ряда {specification}. Ничего не было "
                        f"удалено!")
                else:
                    self.logger.info(f"Ряды таблицы '{from_table} {specification} УДАЛЕНЫ!")
            case _:
                raise NotImplementedError()

    def _get_intersecting_columns(self, from_schema, from_table, to_schema, to_table):
        from_columns = self.conn.get_column_names(self._sql_name(from_schema, from_table))
        to_columns = self.conn.get_column_names(self._sql_name(to_schema, to_table))
        intersect_cols = list(set(from_columns) & set(to_columns))
        formatted_cols = ", ".join(intersect_cols)
        return formatted_cols

    def _get_required_column_name_for_bd(self, schema, table_name, json_column_name):
        """
        Возвращает имя колонки в бд, ориентируясь на данные в JSON
        :param schema:
        :param table_name: json имя таблицы (не то, которое указано в БД)
        :param json_column_name:
        :return: metaload_id_col
        """
        return self.config["tables"][table_name]["req_cols"][json_column_name]

    def _get_required_columns_names_for_bd(self, schema: str,
                                           tables_json_names: list[str],
                                           json_column_name: str) -> list[str]:
        """
        Возвращает имя колонки из БД, для каждой таблицы *tables_json_names*
        :param schema:
        :param tables_json_names:
        :param json_column_name:
        :return:
        """
        metaload_id_cols = []
        for table_name in tables_json_names:
            metaload_id_cols.append(self._get_required_column_name_for_bd(schema,
                                                                          table_name=table_name,
                                                                          json_column_name=json_column_name))
        return metaload_id_cols

    def _normalise_filter_values(self, equal_to_values, columns_names, elements_num):
        if len(equal_to_values) == len(columns_names) == 1:
            self._check_for_filter_types(equal_to_values, columns_names)
            equal_to_values = equal_to_values * elements_num
            columns_names = columns_names * elements_num
        elif len(columns_names) > 1 and len(equal_to_values) == 1:
            equal_to_values = equal_to_values * len(columns_names)
        assert len(
            equal_to_values) == elements_num, f"Ошибка, len(equal_to_values): {len(equal_to_values)}, elements_num: {elements_num}"
        assert len(
            columns_names) == elements_num, f"Ошибка, len(columns_names): {len(columns_names)}, elements_num: {elements_num}"

        return equal_to_values, columns_names

    def _check_for_filter_types(self, equal_to_values, where_cols):
        if type(equal_to_values) != list != type(where_cols):
            raise TypeError(
                f'{self.copy_tables.__name__}: параметры equal_to_values и where_cols обязаны быть list!')

    def _get_db_table_names(self, without=None):
        """
        Возвращает имена колонок в таблице, так как они предположительно отображены в базе данных. Информация
        берется из JSON типа read-only.
        :param without: json имя таблицы
        :return:
        """
        tables = self.config["tables"]
        if without is not None:
            without_name = tables[without]["table_name"]
        else:
            without_name = None

        names = [v["table_name"] for k, v in tables.items() if v["table_name"] != without_name]
        return names

    def _get_json_table_names(self, without=None):
        """
        Возвращает JSON имена колонок в таблице. Эти имена не обязаны соответствовать именам колонок в бд.
        Информация берется из JSON файла типа read-only.
        :param schema:
        :param without: json имя таблицы
        :return:
        """
        tables = self.config["tables"]
        names = [k for k, v in tables.items() if k != without]
        return names

    @staticmethod
    def _sql_name(schema_name, table_name):
        schema_and_table = f"{schema_name}.{table_name}"
        return AsIs(schema_and_table)

    def _query_result_empty(self, data):
        empty_list = len(data) == 0
        none_type = not data

        return any([empty_list, none_type])
