from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'flujo_etl_complejo',
    default_args=default_args,
    description='Un flujo de trabajo ETL completo con títulos descriptivos',
    schedule_interval=timedelta(days=1),
)

inicio = DummyOperator(
    task_id='Inicio_del_proceso_ETL',
    dag=dag,
)

preparacion = PythonOperator(
    task_id='Preparación_de_Datos',
    python_callable=lambda: print("Preparando datos."),
    dag=dag,
)

extraccion = PythonOperator(
    task_id='Extracción_de_Datos',
    python_callable=lambda: print("Extrayendo datos."),
    dag=dag,
)

transformacion = PythonOperator(
    task_id='Transformación_de_Datos',
    python_callable=lambda: print("Transformando datos."),
    dag=dag,
)

carga = PythonOperator(
    task_id='Carga_de_Datos',
    python_callable=lambda: print("Cargando datos."),
    dag=dag,
)

fin = DummyOperator(
    task_id='Fin_del_proceso_ETL',
    dag=dag,
)

inicio >> preparacion >> extraccion >> transformacion >> carga >> fin
