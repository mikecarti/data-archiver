from .common_data_archiver import CommonDataArchiver


class BusinessOrgArchiver(CommonDataArchiver):

    def __init__(self, conn, in_schemas, logger, task_type):
        super().__init__(conn, in_schemas, logger, task_type)

    def run(self, d):
        print("Business Org Archiver running")
        from_table = self.config["main_schema"]["table_name"]
        to_table = self.config["archive_schema"]["table_name"]
        self.archive_table(from_table, to_table)

    def archive_table(self, from_table, to_table, from_schema=None, to_schema=None):
        id_archive_name = self.config["archive_schema"]["columns"][0]
        self.copy_table(from_schema, from_table, to_schema, to_table, id_archive_name)
