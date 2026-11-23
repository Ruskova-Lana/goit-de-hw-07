import random
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.trigger_rule import TriggerRule

try:
    from airflow.providers.common.sql.sensors.sql import SqlSensor
except ImportError:  # старіші версії провайдера
    from airflow.sensors.sql import SqlSensor

# ID підключення до MySQL. Саме підключення (host/user/password) задається
# змінною AIRFLOW_CONN_NEO_DATA_ADMIN у файлі .env, а не в коді
MYSQL_CONN_ID = "neo_data_admin"
# Таблиця для результатів — у схемі neo_data, де є права на запис
RESULTS_TABLE = "neo_data.hw07_medal_counts"
SOURCE_TABLE = "olympic_dataset.athlete_event_results"

MEDALS = ["Bronze", "Silver", "Gold"]
DELAY_SECONDS = 10 # поставте 35, щоб побачити падіння сенсора


def pick_medal_fn():
    medal = random.choice(MEDALS)
    print(f"Обрана медаль: {medal}")
    return medal  # потрапляє в XCom


def pick_medal_branch_fn(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"


def generate_delay_fn():
    print(f"Затримка {DELAY_SECONDS} с")
    time.sleep(DELAY_SECONDS)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="hw07_medals_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["goit", "hw07"],
) as dag:

    # 1. Створення таблиці
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME
        );
        """,
    )

    # 2. Випадковий вибір медалі
    pick_medal = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal_fn,
    )

    # 3. Розгалуження
    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=pick_medal_branch_fn,
    )

    # 4. Підрахунок медалей кожного типу
    calc_tasks = {
        medal: MySqlOperator(
            task_id=f"calc_{medal}",
            mysql_conn_id=MYSQL_CONN_ID,
            sql=f"""
            INSERT INTO {RESULTS_TABLE} (medal_type, count, created_at)
            SELECT '{medal}', COUNT(*), NOW()
            FROM {SOURCE_TABLE}
            WHERE medal = '{medal}';
            """,
        )
        for medal in MEDALS
    }

    # 5. Затримка (виконується, якщо успішна хоча б одна гілка)
    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay_fn,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # 6. Сенсор: найновіший запис не старший за 30 секунд
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
        SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
        FROM {RESULTS_TABLE};
        """,
        mode="poke",
        poke_interval=5,
        timeout=6,
    )

    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_tasks["Silver"], calc_tasks["Gold"], calc_tasks["Bronze"]]
    [calc_tasks["Silver"], calc_tasks["Gold"], calc_tasks["Bronze"]] >> generate_delay
    generate_delay >> check_for_correctness
