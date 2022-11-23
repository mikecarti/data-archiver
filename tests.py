from archivers.common_data_archiver import CommonDataArchiver
import json
import unittest


class TestCommonDataArchiver(unittest.TestCase):
    @staticmethod
    def load_json(path):
        with open(path, 'r', encoding='utf-8') as f:
            json_dict = json.load(f)
        return json_dict

    def test_that_get_table_name_returns_right_for_business_org(self):
        d = self.load_json('json_files/table_data_schemas.json')
        c = CommonDataArchiver(conn=None, in_schemas=d, logger=None)
        name = c.get_table_name("business_orgs_spr")
        self.assertEqual("business_orgs_spr", name)