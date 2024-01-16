# Version 1.0.0
# Разработчик: Боженов Олег

from airflow.models.dagrun import DagRun
from airflow.utils.email import send_email
from airflow.hooks.base import BaseHook

import boto3
import logging
import time
import json

EMAIL = []
EMAIL_test = []

def get_s3_client():
    conn_s3 = BaseHook.get_connection('')
    conn_s3_extra = json.loads(conn_s3.extra)
    session = boto3.session.Session(
        aws_access_key_id=conn_s3_extra['aws_access_key_id'],
        aws_secret_access_key=conn_s3_extra['aws_secret_access_key']
    )
    return session.client(
        service_name='s3',
        endpoint_url=conn_s3.host
    )

def checker(**kwargs):
    checkDAG = kwargs['checkDAG']
    dag_runs = DagRun.find(dag_id=checkDAG)
    states = list()

    while True:
        for dag_run in dag_runs:
            states.append(dag_run.state)
        if 'running' not in states:
            break
        logging.info(f'{checkDAG} still running. Waiting 5 min to complete...')
        time.sleep(300)

        dag_runs = DagRun.find(dag_id=checkDAG)
        states.clear()

def get_all_downstream_tasks(task, dag):
    downstream_tasks = list()
    for downstream_task in task.downstream_list:
        downstream_tasks.append(downstream_task)
        downstream_tasks.extend(get_all_downstream_tasks(downstream_task, dag))
    return list(set(downstream_tasks))

def failure_callback(context):
    task_instance = context['task_instance']
    dag = context['dag']

    failed_task_id = task_instance.task_id
    failed_task = dag.get_task(task_instance.task_id)
    affected_tasks_ids = list()
    if dag.dag_id != "DATAMART-Collection":
        affected_tasks = get_all_downstream_tasks(failed_task, dag)
        for task in affected_tasks:
            if task.task_id != "errorLog":
                affected_tasks_ids.append(task.task_id)

    failure_message = f"{failed_task_id} -> {', '.join(affected_tasks_ids)}"

    ti = context['task_instance']
    current_failure_messages = ti.xcom_pull(task_ids=None, key='failure_messages')

    if current_failure_messages is not None:
        current_failure_messages.append(failure_message)
    else:
        current_failure_messages = [failure_message]

    ti.xcom_push(key='failure_messages', value=current_failure_messages)


def send_failure_notifications(**kwargs):
    ti = kwargs['ti']
    failure_messages = ti.xcom_pull(task_ids=None, key='failure_messages')

    if failure_messages is not None:
        global EMAIL
        try:
            email_third_party = kwargs['email']
            if email_third_party:
                EMAIL += email_third_party
        except KeyError:
            pass

        messages = [msg for msg in failure_messages if msg]

        if messages:
            dag_id = kwargs['dag']
            subject = f"[Airflow] Ошибка в DAG {dag_id}"

            message_head = ("<html> "
                            "<meta charset=\"utf-8\">"
                            "<style> p {font-size: 15pt}</style>")
            message_body = (f"<body><p> Добрый день!"
                            f"<br>Это автоматическое письмо."
                            f"<br>В ходе работы {dag_id} не выполнились следующие задачи:<br>")
            for i in messages:
                message_body += "<br><b>-" + i + "</b>"
            message_footer = ("<br><br>Ошибка на контроле DWH."
                              "<br>После исправления ответственный сотрудник ответным письмом сообщит об этом. </p>"
                              "</body></html>")

            message = message_head + message_body + message_footer
            send_email(EMAIL, subject, message)
