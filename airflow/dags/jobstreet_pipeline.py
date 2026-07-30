from datetime import datetime, timedelta          
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator

PROJ = "/opt/jobstreet"   # where the volume mount put your project

@dag(
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,     
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},   
    tags=["jobstreet"],
)
def jobstreet_pipeline():

    scrape = BashOperator(
        task_id="scrape",
        bash_command=(
            f"mkdir -p {PROJ}/data_lake/raw/jobstreet/date={{{{ ds }}}} && "
            f"cd {PROJ} && "
            "python jobstreet_scraper.py --keywords 'data engineer' --where 'Indonesia' "
            "--out-prefix data_lake/raw/jobstreet/date={{ ds }}/jobs"
        ),
    )

    load_raw = BashOperator(
        task_id="load_raw",
        bash_command=f"cd {PROJ} && python pipeline/load_raw.py {{{{ ds }}}}",
    )

    build_marts = BashOperator(
        task_id="build_marts",
        bash_command=f"cd {PROJ}/transform && dbt build",
    )
    
    export_marts = BashOperator(
        task_id="export_marts",
        bash_command=f"cd {PROJ} && python pipeline/export_marts.py",
    )

    scrape >> load_raw >> build_marts >> export_marts

jobstreet_pipeline()
