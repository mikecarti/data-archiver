from .common_data_archiver import CommonDataArchiver


class ProductGraphArchiver(CommonDataArchiver):

    def __init__(self, conn, in_schemas, logger, task_type):
        super().__init__(conn, in_schemas, logger, task_type)

    def run(self, d):
        print("Product Graph Archiver running")
        from_tables = self.config["main_schema"]["table_names"]
        to_tables = self.config["archive_schema"]["table_names"]
        meta_dataset_id = d["metaload_user_id"]
        metaload_id_col = self.get_metaload_id_col(schema="main_schema")
        #TODO: make archive tables command

        self.archive_tables(from_tables, to_tables, where_cols=[metaload_id_col], equal_to_values=[meta_dataset_id])

    def get_metaload_id_col(self, schema):
        """
        :param schema:
        :return: metaload_id_col
        """
        edges_metaload_id_col = self.in_schemas["archive_product_graphs"][schema]["tables"]["production_graph_edges"]["columns"][0]
        nodes_metaload_id_col = self.in_schemas["archive_product_graphs"][schema]["tables"]["production_graph_nodes"]["columns"][0]

        if edges_metaload_id_col != nodes_metaload_id_col:
            raise KeyError('В файле json table_data_schemas.json, ["archive_product_graphs"]["main_schema"]["tables"]["production_graph_edges"]["columns"] ,'
                           'В файле json table_data_schemas.json, ["archive_product_graphs"]["main_schema"]["tables"]["production_graph_nodes"]["columns"], '
                           'первым элементом должен стоять одинаковый string отображающий "metaload_dataset_id"')

        return edges_metaload_id_col