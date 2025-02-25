from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta
from kubernetes.client import models as k8s  # Kubernetes API 사용

default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
NODE_POOL = "gpu"
container_resources_cuda_gpu = {
    "request_memory": "1G",
    "request_cpu": "100m",
    "limit_gpu": "1",
}
with DAG(
    "tutorial_k8s_affinity",
    default_args=default_args,
    description="A DAG using affinity with KubernetesPodOperator",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    t1 = KubernetesPodOperator(
        task_id="cpu_task_1",
        name="cpu-task-pod-1",
        namespace="airflow",
        image="python:3.8-slim",
        cmds=["python3", "-c"],
        arguments=["import datetime; print('CPU Task 1:', datetime.datetime.now())"],
        is_delete_operator_pod=True,
        in_cluster=True,
        node_selectors={"app": cpu},  # ✅ CPU 노드에서 실행
    )

    t2 = KubernetesPodOperator(
        task_id="gpu_task",
        name="gpu-task-pod",
        namespace="airflow",
        image="python:3.8-slim",
        cmds=["python3", "-c"],
        arguments=["import datetime; print('CPU Task 2:', datetime.datetime.now())"],
        is_delete_operator_pod=True,
        in_cluster=True,
        container_resources=container_resources_cuda_gpu,
        node_selectors={"app": NODE_POOL},
        tolerations=[
            {
             'key': NODE_POOL,
             'operator': 'Equal',
             'value': 'true',
             'effect': "NoSchedule"
            },
            {
             'key': "nvidia.com/gpu",
             'operator': 'Equal',
             'value': 'present',
             'effect': "NoSchedule"
            }
    ],
    )

    # ✅ CPU Task (app=cpu 노드에서 실행) - t3 (t1 & t2 완료 후 실행)
    t3 = KubernetesPodOperator(
        task_id="cpu_task_2",
        name="cpu-task-pod-2",
        namespace="airflow",
        image="python:3.8-slim",
        cmds=["python3", "-c"],
        arguments=["import datetime; print('CPU Task 2:', datetime.datetime.now())"],
        is_delete_operator_pod=True,
        in_cluster=True,
        node_selectors={"app": cpu},  # ✅ CPU 노드에서 실행
    )

    # ✅ 실행 순서 정의
    [t1, t2] >> t3  # ✅ t1 & t2 병렬 실행 후 t3 실행
