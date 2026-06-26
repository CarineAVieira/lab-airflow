from __future__ import annotations

from airflow.decorators import dag, task


def on_success_callback(context):
    task_id = context['task'].task_id
    print(f"✓ Task {task_id} completou com sucesso!")


def on_failure_callback(context):
    task_id = context['task'].task_id
    exception = context['exception']
    print(f"✗ Task {task_id} falhou: {exception}")


def on_retry_callback(context):
    task_id = context['task'].task_id
    print(f"↻ Task {task_id} vai fazer retry")


@dag(dag_id='callbacks_example', schedule=None, start_date=None)
def pipeline():

    @task(
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        on_retry_callback=on_retry_callback,
        retries=2,
    )
    def critical_task():
        pass

    critical_task()


pipeline()
