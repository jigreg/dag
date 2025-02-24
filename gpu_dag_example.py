from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'tutorial_k8s',
    default_args=default_args,
    description='A tutorial DAG with KubernetesPodOperator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # ✅ 일반 CPU Task (app:cpu 라벨이 있는 노드에서 실행)
    t1 = KubernetesPodOperator(
        task_id='print_date',
        name="print-date-pod",
        namespace="airflow",
        image="python:3.8-slim",
        cmds=["python3", "-c"],
        arguments=["import datetime; print(datetime.datetime.now())"],
        labels={"app": "cpu"},
        is_delete_operator_pod=True,
        executor_config={
            "pod_override": {
                "nodeSelector": {
                    "app": "cpu"  # CPU 노드에서 실행
                }
            }
        },
    )

    t2 = KubernetesPodOperator(
        task_id='sleep',
        name="sleep-pod",
        namespace="airflow",
        image="ubuntu",
        cmds=["bash", "-c"],
        arguments=["sleep 5"],
        labels={"app": "cpu"},
        is_delete_operator_pod=True,
        executor_config={
            "pod_override": {
                "nodeSelector": {
                    "app": "cpu"  # CPU 노드에서 실행
                }
            }
        },
    )

    # ✅ GPU Task (app:gpu 라벨이 있는 노드에서 실행)
    t3_gpu = KubernetesPodOperator(
        task_id='gpu_task',
        name="gpu-task-pod",
        namespace="airflow",
        image="nvidia/cuda:11.4.2-runtime-ubuntu20.04",
        cmds=["bash", "-c"],
        arguments=["nvidia-smi"],
        labels={"app": "gpu"},
        is_delete_operator_pod=True,
        executor_config={
            "pod_override": {
                "nodeSelector": {
                    "app": "gpu"  # GPU 노드에서 실행
                }
        },
    )

    templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """

    t4 = KubernetesPodOperator(
        task_id='templated',
        name="templated-pod",
        namespace="airflow",
        image="ubuntu",
        cmds=["bash", "-c"],
        arguments=[templated_command],
        labels={"app": "cpu"},
        is_delete_operator_pod=True,
        executor_config={
            "pod_override": {
                "nodeSelector": {
                    "app": "cpu"  # CPU 노드에서 실행
                }
            }
        },
    )

    # ✅ 실행 순서 정의
    t1 >> [t2, t3_gpu, t4]
