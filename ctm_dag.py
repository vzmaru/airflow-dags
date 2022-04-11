from datetime import datetime
from airflow import DAG
from fmr.pi.fili.airflow.sensors.ctm import CtmConditionSensorAsync
from fmr.pi.fili.airflow.sensors.custom import MyCustomSensorAsync

with DAG(
   "ctm_dag",
   start_date=datetime(2022, 4, 10, 0, 0),
   end_date=datetime(2022, 5, 31, 23, 59),
   schedule_interval="0 12 * * *",
   catchup=True,
   max_active_runs=32,
   max_active_tasks=32
) as dag:

    ctm_task_1 = CtmConditionSensorAsync(
        task_id="ctm_task_1",
        event_name="x",
    )

    ctm_task_2 = MyCustomSensorAsync(
        task_id="ctm_task_2",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=1) }}""",
    )

    ctm_task_2 >> ctm_task_1
