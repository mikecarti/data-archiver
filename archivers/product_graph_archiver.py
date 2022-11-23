from .common_data_archiver import CommonDataArchiver


class ProductGraphArchiver(CommonDataArchiver):

    def __init__(self, conn, in_schemas, logger, task_type):
        super().__init__(conn, in_schemas, logger, task_type)

    def run(self, d):
        print("Product Graph Archiver running")
        from_tables = self.config["main_schema"]["table_names"]
        to_tables = self.config["archive_schema"]["table_names"]
        self.archive_tables(from_tables, to_tables)
#       #TODO: make archive tables command
