from .common_data_archiver import CommonDataArchiver


class ProductGraphArchiver(CommonDataArchiver):

    def __init__(self, conn, in_schemas, logger, task_type):
        super().__init__(conn, in_schemas, logger, task_type)

    def run(self, d):
        print("Product Graph Archiver running")
        self.meta_dataset_id = d["metaload_dataset_id"]
        self.archive_tables()

    def archive_tables(self):
        self.copy_metadata_entry()
        self.copy_production_graph_tables()
        self.delete_production_graph_tables()

    def copy_production_graph_tables(self):
        from_tables = self.get_json_table_names("main_schema", without="upload_files")
        to_tables = self.get_json_table_names("main_schema", without="upload_files")

        metaload_id_col = self.get_metaload_id_col(schema="main_schema")
        id_archive_name = self.get_archivation_id_col()
        self.copy_tables(from_tables, to_tables, id_archive_name, where_cols=[metaload_id_col],
                         equal_to_values=[self.meta_dataset_id])

    # TODO: Добавить удаление данных (пункт 3)

    def copy_metadata_entry(self):
        """
        :param meta_dataset_id: ID ряда в upload_files, который будет скопирован в архив
        :return:
        """
        pub_upload = self.config["main_schema"]["tables"]["upload_files"]
        arch_upload = self.config["archive_schema"]["tables"]["upload_files"]

        from_table = pub_upload["table_name"]
        to_table = arch_upload["table_name"]
        archive_id_col = arch_upload["req_cols"]["archive_id"]
        meta_dataset_col = pub_upload["req_cols"]["metaload_dataset_id"]

        self.copy_table(from_table, to_table, archive_id_col,
                   where_col=meta_dataset_col, equals_to=self.meta_dataset_id)

    def delete_production_graph_tables(self):
        delete_tables = self.get_json_table_names("main_schema", without="upload_files")

    def get_archivation_id_col(self):
        id_archive_name_nodes = self.config["archive_schema"]["tables"]["production_graph_nodes"]["req_cols"]["archive_id"]
        id_archive_name_edges = self.config["archive_schema"]["tables"]["production_graph_edges"]["req_cols"]["archive_id"]
        if id_archive_name_edges != id_archive_name_nodes:
            raise KeyError("разные наименования id_archive_name в edges и nodes в json")
        return id_archive_name_nodes

    def get_metaload_id_col(self, schema):
        """
        :param schema:
        :return: metaload_id_col
        """
        edges_metaload_id_col = self.in_schemas["archive_production_graph"][schema]["tables"]["production_graph_edges"]["req_cols"]["metaload_dataset_id"]
        nodes_metaload_id_col = self.in_schemas["archive_production_graph"][schema]["tables"]["production_graph_edges"]["req_cols"]["metaload_dataset_id"]

        if edges_metaload_id_col != nodes_metaload_id_col:
            raise KeyError('В файле json table_data_schemas.json, ["archive_production_graph"]["main_schema"]["tables"]["production_graph_edges"]["columns"] ,'
                           'В файле json table_data_schemas.json, ["archive_production_graph"]["main_schema"]["tables"]["production_graph_nodes"]["columns"], '
                           'первым элементом должен стоять одинаковый string отображающий "metaload_dataset_id"')

        return edges_metaload_id_col

