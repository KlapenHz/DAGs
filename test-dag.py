# from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import # for airflow > 2.0
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.kubernetes.secret import Secret
# from kubernetes.client import models as k8s # only for airflow 2.x

from airflow.utils.dates import days_ago

default_ns = "airflow-cluster"
default_args = {
    'start_date': days_ago(1),
    'retries': 0,
    'retries_delay': timedelta(seconds=60),
}
secret_file = Secret('volume', '/secret-file.txt', 'my_secret', 'my-key')


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

    code_volume = k8s.V1Volume(
        name="code-source-volume",
        config_map=k8s.V1ConfigMapVolumeSource(
            name="code-source-volume",
            optional=False,
            items=[k8s.V1KeyToPath(key="code", path="code.b64")],
        ),
    ),
    #code_dst_volume = k8s.V1Volume(name="code-volume", empty_dir={}),
    #code_dst_volume_mount = k8s.V1VolumeMount(mount_path="/code", name="code-volume"),
    #code_volume_mount = k8s.V1VolumeMount(mount_path="/code-zipped", name="code-source-volume", read_only=False),

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
            # volume_mounts=airflow_volume_mounts,
            volumes=[
                Volume(name="test-dir", configs={"hostPath": {"path": "/mnt/dags"}}),
                #Volume(name="files-volume", empty_dir={}),
            ],
            volume_mounts=[
                VolumeMount("test-dir", mount_path="/myInsideDags", sub_path=None, read_only=True),
                #VolumeMount(mount_path="/files", name="files-volume"),
            ],
            # volume=k8s.V1ConfigMapVolumeSource(name="configtest", items=[V1KeyToPath(key='bar', path='foo')]),
            #volumes=[code_volume, k8s.V1Volume(name="code-volume", empty_dir={})],
            #volume_mounts=[code_volume_mount, code_dst_volume_mount],
            #volume_mounts=[
            #    VolumeMount(mount_path="/code-zipped", name="code-source-volume", read_only=False),
            #    VolumeMount(mount_path="/code", name="code-volume"),
            #],
        )


create_job()
