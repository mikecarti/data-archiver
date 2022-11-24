from .common_data_archiver import CommonDataArchiver


class BusinessOrgArchiver(CommonDataArchiver):

    def __init__(self, conn, in_schemas, logger, task_type):
        super().__init__(conn, in_schemas, logger, task_type)

    def run(self, d):
        print("Business Org Archiver running")
        from_table = list(self.config["main_schema"]["tables"].keys())[0]
        to_table = list(self.config["archive_schema"]["tables"].keys())[0]
        self.archive_table(from_table, to_table)

    def archive_table(self, from_table, to_table):
        # id_archive_name = self.conn.get_column_names(to_table, self.config["archive_schema"]["schema_name"])
        id_archive_name = self.config["archive_schema"]["tables"]["business_orgs_spr"]["req_cols"]["archivation_id"]
        self.copy_table(from_table, to_table, id_archive_name)