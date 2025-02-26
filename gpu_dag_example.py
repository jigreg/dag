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

with DAG(
    "tutorial_k8s_affinity",
    default_args=default_args,
    description="A DAG using affinity with KubernetesPodOperator",
    tags=["example"],
) as dag:

    # ✅ CPU Task (app=cpu 노드에서 실행)
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

    t1 = KubernetesPodOperator(
        task_id="cpu_task",
        name="cpu-task-pod",
        namespace="airflow",
        image="python:3.8-slim",
        cmds=["python3", "-c"],
        arguments=["import datetime; print(datetime.datetime.now())"],
        is_delete_operator_pod=True,
        in_cluster=True,
        affinity=cpu_affinity,  # ✅ CPU 노드에서만 실행됨
    )

    # ✅ GPU Task (app=gpu 노드에서 실행)
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

    t2 = KubernetesPodOperator(
        task_id="gpu_task",
        name="gpu-task-pod",
        namespace="airflow",
        image="nvidia/cuda:11.4.2-runtime-ubuntu20.04",
        cmds=["bash", "-c"],
        arguments=["nvidia-smi"],
        is_delete_operator_pod=True,
        in_cluster=True,
        affinity=gpu_affinity,  # ✅ GPU 노드에서만 실행됨
    )

    t1 >> t2
