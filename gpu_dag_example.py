from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'tutorial',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
        executor_config={
            "pod_override": {
                "nodeSelector": {"app": "cpu"}
            },
        },
    )
    
    t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
        executor_config={
            "pod_override": {
                "nodeSelector": {"app": "cpu"}
            },
        },
    )

    # GPU 전용 Task
    t3_gpu = BashOperator(
        task_id='gpu_task',
        bash_command='echo "Running on GPU node"',
        executor_config={
            "pod_override": {
                "nodeSelector": {
                    "app": "gpu"  # GPU 노드에서만 실행
                }
            }
        },
    )

    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    t4 = BashOperator(
    task_id='templated',
    bash_command=templated_command,
    executor_config={
        "pod_override": {
            "nodeSelector": {"app": "cpu"}
        },
    },
)

    # 실행 순서
    t1 >> [t2, t3_gpu, t4]
