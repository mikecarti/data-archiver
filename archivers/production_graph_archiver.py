from .common_data_archiver import CommonDataArchiver


class ProductGraphArchiver(CommonDataArchiver):

    def __init__(self, conn, in_schemas, logger, task_type):
        super().__init__(conn, in_schemas, logger, task_type)
        self.meta_dataset_id = None

    def run(self, d: dict):
        print("Product Graph Archiver running")
        self.meta_dataset_id = d["metaload_dataset_id"]

        return self.common_run(task_type=d['file_type'])

    def archive_tables(self):
        self.copy_metadata_entry()
        self.copy_production_graph_tables()
        self.delete_production_graph_tables()
        self.delete_metadata_entry()


    def copy_production_graph_tables(self):
        from_tables = self.get_json_table_names("main_schema", without="upload_files")
        to_tables = self.get_json_table_names("main_schema", without="upload_files")

        metaload_id_col = self.get_required_column_name_for_bd("main_schema",
                                                                table_name="production_graph_edges",
                                                                json_column_name="metaload_dataset_id")
        self.copy_tables(from_tables, to_tables, where_cols=[metaload_id_col],
                         equal_to_values=[self.meta_dataset_id])

    def copy_metadata_entry(self):
        """
        :return:
        """
        pub_upload = self.config["main_schema"]["tables"]["upload_files"]
        arch_upload = self.config["archive_schema"]["tables"]["upload_files"]

        from_table = pub_upload["table_name"]
        to_table = arch_upload["table_name"]
        meta_dataset_col = pub_upload["req_cols"]["metaload_dataset_id"]

        self.copy_table(from_table, to_table,
                        where_col=meta_dataset_col, equals_to=self.meta_dataset_id)

    def delete_production_graph_tables(self):
        delete_tables = self.get_json_table_names("main_schema", without="upload_files")
        metaload_id_col = self.get_required_column_name_for_bd("main_schema",
                                                                table_name="production_graph_edges",
                                                                json_column_name="metaload_dataset_id")

        self.delete_tables(delete_tables, where_cols=[metaload_id_col], equal_to_values=[self.meta_dataset_id])

    def delete_metadata_entry(self):
        upload_files_ = self.config["main_schema"]["tables"]["upload_files"]
        table = upload_files_["table_name"]
        meta_dataset_col = upload_files_["req_cols"]["metaload_dataset_id"]
        self.delete_table(table=table, where_col=meta_dataset_col, equal_to=self.meta_dataset_id)

    def get_required_column_name_for_bd(self, schema, table_name, json_column_name):
        """
        Возвращает имя колонки в бд, ориентируясь на данные в JSON
        :param schema:
        :param table_name:
        :param json_column_name:
        :return: metaload_id_col
        """
        return self.in_schemas[self.task_type][schema]["tables"][table_name]["req_cols"][json_column_name]


