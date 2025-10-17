from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime
from airflow.models.dag import DAG
import os

DBT_PROJECT_PATH = "/usr/local/airflow/include/dbt/online_retail_II"
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv/bin/dbt"

project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_PATH)

profile_config = ProfileConfig(
    profile_name="online_retail_II",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="online_retail_db",
        profile_args={"schema": "raw"},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

with DAG(
    dag_id="online_retail_II_dag",
    start_date=datetime(2025, 10, 16),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:

    # 🟩 1️⃣ Run models
    dbt_run = DbtTaskGroup(
        group_id="dbt_run",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "install_deps": True,
            "select": ["path:models/staging", "path:models/marts"],
            "full_refresh": True,
        },
    )
