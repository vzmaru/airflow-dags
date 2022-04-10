from datetime import datetime
from airflow import DAG
from fmr.pi.fili.airflow.sensors.custom import MyCustomSensorAsync
 
with DAG(
   "async_dag",
   start_date=datetime(2021, 12, 22, 20, 0),
   end_date=datetime(2021, 12, 22, 20, 19),
   schedule_interval="* * * * *",
   catchup=True,
   max_active_runs=32,
   max_active_tasks=32
) as dag:
 
   async_sensor = MyCustomSensorAsync(
       task_id="async_task",
       target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=5) }}""",
   )
