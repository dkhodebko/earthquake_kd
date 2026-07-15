"""
Ежедневное удаление данных из XCom, которые хранятся больше суток, со всех DAGов
"""

from airflow import DAG
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger('airflow.task')

DEFAULT_ARGS = {
    'owner': 'kd',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2026, 7, 7),
}

with DAG(
    dag_id='TECH_Clean_XComs',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    description='Ежедневное удаление данных из XCom',
    tags=['TECH', 'XCom', 'cleaner'],
) as dag:

    dag.doc_md = __doc__

    @provide_session
    def cleanup_xcoms(session=None, **context):
        # Удаляет все XCom, которые старше 1 дня
        num_rows_deleted = 0
        date_limit = context['logical_date'] - timedelta(days=1)
        logger.info(f"Удаление XCom вплоть до {date_limit}")
        try:
            num_rows_deleted = (
                session.query(XCom).filter(XCom.timestamp <= date_limit).delete()
            )
            session.commit()
        except:
            session.rollback()
        
        if num_rows_deleted == 0:
            logger.info("Нет XCom для удаления")
        else:
            logger.info(f"Удалено {num_rows_deleted} XCom")
    
    start_task = EmptyOperator(task_id='start')

    clean_xcoms_task = PythonOperator(
        task_id='cleanup_xcoms',
        python_callable=cleanup_xcoms,
        provide_context=True,
    )

    end_task = EmptyOperator(task_id='end')

    start_task >> clean_xcoms_task >> end_task