from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta
from kubernetes.client import models as k8s  # Kubernetes API 사용

# 기본 DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gpu_cpu_task_example",
    default_args=default_args,
    description="A DAG using KubernetesPodOperator with node affinity",
    start_date=datetime(2024, 2, 28),
    schedule_interval=None,  # 필요 시 변경 가능 (예: timedelta(days=1))
    catchup=False,
    tags=["k8s", "gpu", "cpu"],
) as dag:

    # ✅ CPU Task (app=cpu 태그를 가진 노드에서 실행)
    cpu_affinity = k8s.V1Affinity(
        node_affinity=k8s.V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
                node_selector_terms=[
                    k8s.V1NodeSelectorTerm(
                        match_expressions=[
                            k8s.V1NodeSelectorRequirement(
                                key="app",
                                operator="In",
                                values=["cpu"],
                            )
                        ]
                    )
                ]
            )
        )
    )

    cpu_task = KubernetesPodOperator(
        task_id="cpu_task",
        name="cpu-task-pod",
        namespace="airflow",
        image="python:3.8-slim",
        cmds=["python3", "-c"],
        arguments=["import datetime; print(f'CPU Task executed at: {datetime.datetime.now()}')"],
        is_delete_operator_pod=True,
        in_cluster=True,
        affinity=cpu_affinity,  # ✅ CPU 노드에서 실행됨
    )

    # ✅ GPU Task (app=gpu 태그를 가진 노드에서 실행)
    gpu_affinity = k8s.V1Affinity(
        node_affinity=k8s.V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
                node_selector_terms=[
                    k8s.V1NodeSelectorTerm(
                        match_expressions=[
                            k8s.V1NodeSelectorRequirement(
                                key="app",
                                operator="In",
                                values=["gpu"],
                            )
                        ]
                    )
                ]
            )
        )
    )

    gpu_task = KubernetesPodOperator(
        task_id="gpu_task",
        name="gpu-task-pod",
        namespace="airflow",
        image="nvidia/cuda:12.8.0-base-ubuntu20.04",
        cmds=["nvnida-smi"],
        # arguments=["import datetime; print(f'GPU Task executed at: {datetime.datetime.now()}')"],
        is_delete_operator_pod=True,
        in_cluster=True,
        affinity=gpu_affinity,  # ✅ GPU 노드에서 실행됨
        # resources=k8s.V1ResourceRequirements(
        #     limits={"nvidia.com/gpu": "1", "memory": "2Gi"},
        #     requests={"nvidia.com/gpu": "1", "memory": "1Gi"},
        # ),
        env_vars={"TASK_TYPE": "GPU"},
    )

    # 실행 순서 지정 (CPU 태스크가 완료된 후 GPU 태스크 실행)
    cpu_task >> gpu_task
