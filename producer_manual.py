import pika
import json
import os
import random


def run(rabbit_host, rabbit_port):
    connection_params = pika.ConnectionParameters(rabbit_host, rabbit_port)
    conn = pika.BlockingConnection(connection_params)
    ch = conn.channel()
    ch.queue_declare(queue="archive_queue", durable=False)
    task_id = random.randint(1, 1000000)
    data_folder = r"/var/lib/data_folder"

    # REPORT
    # data = {
    #     "task_id": task_id,
    #     "file_type": "production_graph_report",
    #     "metaload_dataset_id": 1,
    #     "file_id": -1
    # }
    # ch.basic_publish(
    #     exchange="",
    #     routing_key="task_queue",
    #     body=json.dumps(data)
    # )
    # print(f"[x] Sent message {json.dumps(data)}")


    file_loc = r"1_year/scenario/production_graph (1).xlsx"
    # data = {
    #     "task_id": task_id,
    #     "metaload_user_id": 1,
    #     "metaload_dataset_id": 3,
    #     "file_type": "archive_business_orgs_spr",
    #     "file_type_name": "Производственный граф",
    #     "file_id": -1,
    #     "filename": "some_name",
    #     "metaload_comment": "Заархивирован файл: фыадлыжадлфркрф",
    # }
    data = {
        "task_id": task_id,
        "metaload_user_id": 1,
        "metaload_dataset_id": 3,
        "file_type": "archive_production_graph",
        "file_type_name": "Производственный граф",
        "file_id": -1,
        "filename": "some_name",
        "metaload_comment": "Заархивирован файл: фыадлыжадлфркрф",
    }
    ch.basic_publish(
        exchange="",
        routing_key="archive_queue",
        body=json.dumps(data)
    )
    print(f"[x] Sent message {json.dumps(data)}")



    # file_loc = r"1_year/scenario/План по МЗ.xlsx"
    # data = {
    #     "task_id": task_id,
    #     "metaload_user_id": 1,
    #     "file_upload_path": file_loc,
    #     "start_year": 2024,
    #     "dataset_source_code": "Эксель",
    #     "file_type": "production_plan",
    #     "file_type_name": "Производственный план",
    #     "file_id": -1,
    #     "filename": "some_name",
    #     "metaload_comment": "Загружен файл: лывимровыолмривлымирлыовирмловирмлывоирмлрыиморивмлирвымрио",
    # }
    # ch.basic_publish(
    #     exchange="",
    #     routing_key="task_queue",
    #     body=json.dumps(data)
    # )
    # print(f"[x] Sent message {json.dumps(data)}")



    # file_loc = r"1_year/scenario/invest_event_kkr.xlsx"
    # data = {
    #     "task_id": task_id,
    #     "metaload_user_id": 1,
    #     "file_upload_path": file_loc,
    #     "dataset_source_code": "Эксель",
    #     "file_type": "invest_event",
    #     "file_type_name": "Инвест программа",
    #     "file_id": -1,
    #     "filename": "some_name",
    #     "metaload_comment": "Загружен файл: вирмлывоирмлрыиморивмлирвымрио",
    # }
    # ch.basic_publish(
    #     exchange="",
    #     routing_key="task_queue",
    #     body=json.dumps(data)
    # )
    # print(f"[x] Sent message {json.dumps(data)}")




    # data = {
    #     "task_id": task_id,
    #     "metaload_dataset_id":{"plan":{"id":30},"graph":{"id":2}},
    #     "file_type": "graph_plan_checker",
    #     "file_id": -1 # Лишняя херня для сокета
    #
    # }
    # ch.basic_publish(
    #     exchange="",
    #     routing_key="task_queue",
    #     body=json.dumps(data)
    # )
    # print(f"[x] Sent message {json.dumps(data)}")



    # data = {
    #     "task_id": task_id,
    #     "metaload_dataset_id":{"plan":{"id":30},"graph":{"id":2}, "macro": {"id":1}},
    #     "file_type": "scenario_checker",
    #     "file_id": -1 # Лишняя херня для сокета
    #
    # }
    # ch.basic_publish(
    #     exchange="",
    #     routing_key="task_queue",
    #     body=json.dumps(data)
    # )
    # print(f"[x] Sent message {json.dumps(data)}")




    # file_loc = r"1_year/scenario/failure_schedule.xlsx"
    # data = {
    #     "task_id": task_id,
    #     "metaload_user_id": 1,
    #     "file_upload_path": file_loc,
    #     "start_year": 2024,
    #     "dataset_source_code": "Эксель",
    #     "file_type": "failure_schedule",
    #     "file_type_name": "Запланированные события",
    #     "file_id": -1,
    #     "filename": "some_name",
    #     "metaload_comment": "Загружен файл: лывимровыолмривлымирлыовирмловирмлывоирмлрыиморивмлирвымрио",
    # }
    # ch.basic_publish(
    #     exchange="",
    #     routing_key="task_queue",
    #     body=json.dumps(data)
    # )
    # print(f"[x] Sent message {json.dumps(data)}")


    # file_loc = r"1_year/scenario/macroeconomic.xlsx"
    # data = {
    #     "task_id": task_id,
    #     "metaload_user_id": 1,
    #     "file_upload_path": file_loc,
    #     "start_year": 2024,
    #     "dataset_source_code": "Эксель",
    #     "file_type": "macroeconomics",
    #     "file_type_name": "Макроэкономика",
    #     "file_id": -1,
    #     "filename": "some_name",
    #     "metaload_comment": "Загружен файл: ирвымрио",
    # }
    # ch.basic_publish(
    #     exchange="",
    #     routing_key="task_queue",
    #     body=json.dumps(data)
    # )
    # print(f"[x] Sent message {json.dumps(data)}")


    # file_loc = r"1_year/scenario/cost.xlsx"
    # data = {
    #     "task_id": task_id,
    #     "metaload_user_id": 1,
    #     "file_upload_path": file_loc,
    #     "start_year": 2024,
    #     "dataset_source_code": "Эксель",
    #     "file_type": "cost",
    #     "file_type_name": "Себестоимость",
    #     "file_id": -1,
    #     "filename": "some_name",
    #     "metaload_comment": "Загружен файл: лывимролрыиморивмлирвымрио",
    # }
    # ch.basic_publish(
    #     exchange="",
    #     routing_key="task_queue",
    #     body=json.dumps(data)
    # )
    # print(f"[x] Sent message {json.dumps(data)}")


    # file_loc = r"Spravochniki/products_spr.xlsx"
    # # file_upload_path = os.path.join(data_folder, file_loc)
    # data = {
    #     "task_id": task_id,
    #     "metaload_user_id": 1,
    #     "file_upload_path": file_loc,
    #     "file_type": f"spravochnik_products_spr",
    #     "file_type_name": "Справочник БЕ",
    #     "file_id":-1,
    #     "filename": "some_name",
    #     "metaload_comment": "Загружен файл: лывимррвымрио",
    #     "data_send": "2022-09-14",
    #     "dataset_source_code": "EXCEL"
    # }
    # ch.basic_publish(
    #     exchange="",
    #     routing_key="task_queue",
    #     body=json.dumps(data)
    # )
    # print(f"[x] Sent message {json.dumps(data)}")


    # data_folder_spr= r"C:\PythonProjects\EBITDA-PingCRM\ebitda-data-manager\test_common_data\upload_files\Spravochniki"
    # files = os.listdir(data_folder_spr)
    # spr_list = []
    # with DBCon(db_name, db_user, db_pass, db_host, db_port) as db_conn:
    #     for file in files:
    #         if os.path.isfile(os.path.join(data_folder_spr, file)) and file.split('.')[-1]=='xlsx':
    #             spr_list.append(file)
    #
    #     for file in spr_list:
    #         # task_id = db_conn.set_new_task("task_test", 1, '{}')
    #         task_id = random.randint(1, 1000000)
    #         file_loc = os.path.join(r"Spravochniki/", file)
    #         data = {
    #             "task_id": task_id,
    #             "metaload_user_id": 1,
    #             "file_upload_path": file_loc,
    #             "file_type": f"spravochnik_{file.split('.')[0]}",
    #             "file_type_name": "Справочник валюты",
    #             "file_id": -1,
    #             "filename": "some_name",
    #             "metaload_comment": "Загружен файл: лывимровыолмривлымирлыовирмловирмлывоирмлрыиморивмлирвымрио",
    #             "data_send": "2022-09-14",
    #             "dataset_source_code": "Эксель",
    #
    #         }
    #         ch.basic_publish(
    #             exchange="",
    #             routing_key="task_queue",
    #             body=json.dumps(data)
    #         )
    #         print(f"[x] Sent message {json.dumps(data)}")

    conn.close()


if __name__ == "__main__":
    rabbit_broker = os.environ['RABBIT_HOST'] if 'RABBIT_HOST' in os.environ else "localhost"
    rabbit_port = int(os.environ['RABBIT_PORT']) if 'RABBIT_PORT' in os.environ else 15555
    db_name = os.environ['DB_NAME'] if 'DB_NAME' in os.environ else "ebitda"
    db_user = os.environ['DB_USER'] if 'DB_USER' in os.environ else "postgres"
    db_pass = os.environ['DB_PASS'] if 'DB_PASS' in os.environ else "guest"
    db_host = os.environ['DB_HOST'] if 'DB_HOST' in os.environ else "localhost"
    db_port = int(os.environ['DB_PORT']) if 'DB_PORT' in os.environ else 5482  # для теста на локалке


    run(rabbit_host=rabbit_broker, rabbit_port=15555)
