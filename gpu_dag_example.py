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

# ✅ CPU / GPU 노드 설정
CPU_NODE_POOL = "cpu"
GPU_NODE_POOL = "gpu"

# ✅ GPU Task 리소스 설정
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

    # ✅ CPU Task 1 (app=cpu 노드에서 실행)
    t1 = KubernetesPodOperator(
        task_id="cpu_task_1",
        name="cpu-task-pod-1",
        namespace="airflow",
        image="python:3.8-slim",
        cmds=["python3", "-c"],
        arguments=["import datetime; print('CPU Task 1:', datetime.datetime.now())"],
        is_delete_operator_pod=True,
        in_cluster=True,
        node_selectors=["app": CPU_NODE_POOL],  # ✅ CPU 노드에서 실행되도록 설정
    )

    # ✅ GPU Task (app=gpu 노드에서 실행)
    t2 = KubernetesPodOperator(
        task_id="gpu_task",
        name="gpu-task-pod",
        namespace="airflow",
        image="nvidia/cuda:12.8.0-runtime-ubuntu20.04",  # ✅ GPU 연산을 위한 CUDA 이미지 사용
        cmds=["bash", "-c"],
        arguments=["nvidia-smi && sleep 30"],  # ✅ GPU 상태 확인 후 30초 대기
        is_delete_operator_pod=True,
        in_cluster=True,
        container_resources=container_resources_cuda_gpu,
        node_selectors={"app": GPU_NODE_POOL},  # ✅ GPU 노드에서만 실행
    )

    # ✅ CPU Task 2 (app=cpu 노드에서 실행) - t3 (t1 & t2 완료 후 실행)
    t3 = KubernetesPodOperator(
        task_id="cpu_task_2",
        name="cpu-task-pod-2",
        namespace="airflow",
        image="python:3.8-slim",
        cmds=["python3", "-c"],
        arguments=["import datetime; print('CPU Task 2:', datetime.datetime.now())"],
        is_delete_operator_pod=True,
        in_cluster=True,
        node_selectors={"app": CPU_NODE_POOL},  # ✅ CPU 노드에서 실행되도록 설정
    )

    # ✅ 실행 순서 정의
    [t1, t2] >> t3  # ✅ t1 & t2 병렬 실행 후 t3 실행
