from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.models import DAG
from datetime import timedelta
from airflow.kubernetes.secret import Secret

from airflow.utils.dates import days_ago

default_ns = "airflow-cluster"
default_args = {
    'start_date': days_ago(1),
    'retries': 0,
    'retries_delay': timedelta(seconds=60),
}
secret_file = Secret('volume', '/secret-file.txt', 'mysecret', 'someusername')


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
            "request_memory": "256Mi",
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
            image="bash",
            labels={**job_labels},
            secrets=[secret_file],
            cmds=["bash", "-c"],
            arguments=["sleep 300"],
            volumes=[
                Volume(name="test-dir", configs={"hostPath": {"path": "/mnt/dags"}}),
            ],
            volume_mounts=[
                VolumeMount("test-dir", mount_path="/myInsideDags", sub_path=None, read_only=True),
            ],
        )


create_job()
