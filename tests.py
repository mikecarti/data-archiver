import time
import unittest
import random
import producer
import os
import main
import threading
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
    def test_business_org_archiver_run(self):

        d = self.get_data("archive_business_orgs_spr")
        producer.run(rabbit_host=self.rabbit_broker, rabbit_port=15555, data=d)
    def test_production_graph_archiver_run(self):
        d = self.get_data("archive_production_graph")
        producer.run(rabbit_host=self.rabbit_broker, rabbit_port=15555, data=d)
