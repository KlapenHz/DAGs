from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.models import DAG
from datetime import timedelta, datetime

from airflow.utils.dates import days_ago

default_ns = "default"
default_args = {
    'start_date': days_ago(1),
    'retries': 0,
    'retries_delay': timedelta(seconds=60),
}


def create_dag(schedule):
    dag_id = 'ek-example'
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule)
    globals()[dag_id] = dag
    return dag


def create_job(
        schedule=None,
        resources={
            "request_cpu": "100m",
            "limit_cpu": "1",
            "request_memory": "256Mb",
            "limit_memory": "1Gi",
        },
):
    job_name = "ek-dag-test"
    job_labels = {"app": job_name}
    env_vars = {
        "SHELL": "/bin/bash"
    }

    with create_dag(schedule):
        KubernetesPodOperator(
            get_logs=True,
            task_id=job_name,
            name=job_name,
            namespace=default_ns,
            resources=resources,
            image="alpine",
            labels={**job_labels},
            cmds=["bash", "-c"],
            arguments=["echo test"],
            volumes=[
                Volume(name="test-dir", configs={"hostPath": {"path": "/mnt/dags"}}),
            ],
            volume_mounts=[
                VolumeMount("test-dir", mount_path="/mountDags", sub_path=None, read_only=True),
            ]
        )


create_job()
