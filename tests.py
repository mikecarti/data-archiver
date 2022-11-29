import time
import unittest
import random
import producer
import os
import main
import threading

from archivers.common_data_archiver import CommonDataArchiver
from interface_data_archiver import DataArchiver

class TestDataArchiver(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        thr = threading.Thread(target=main.main, args=(), kwargs={})
        thr.start()
        time.sleep(2)

    def setUp(self):
        rabbit_broker = os.environ['RABBIT_HOST'] if 'RABBIT_HOST' in os.environ else "localhost"
        self.rabbit_broker = rabbit_broker
        self.data = {
            "task_id": random.randint(1, 1000000),
            "metaload_user_id": 1,
            "file_type": "",
            "metaload_dataset_id": 3,
            "file_type_name": "Производственный граф",
            "file_id": -1,
            "filename": "some_name",
            "metaload_comment": "Заархивирован файл: фыадлыжадлфркрф",
        }


    def get_data(self, type):
        data = self.data
        data["file_type"] = type
        return data

    def get_json_data(self):
        d = DataArchiver.parse_json('json_files/table_data_schemas.json')
        return d

    def test_get_json_table_names_return_table_names(self):
        d = self.get_json_data()
        c = CommonDataArchiver(None, in_schemas=d, logger=None, task_type="archive_production_graph")
        names = c.get_json_table_names("main_schema")
        self.assertEqual(["production_graph_edges", "production_graph_nodes", "upload_files"], names)

    def test_get_json_table_names_return_table_names_without_some_name(self):
        d = self.get_json_data()
        c = CommonDataArchiver(None, in_schemas=d, logger=None, task_type="archive_production_graph")
        names = c.get_json_table_names("main_schema", without="upload_files")
        self.assertEqual(["production_graph_edges", "production_graph_nodes"], names)

