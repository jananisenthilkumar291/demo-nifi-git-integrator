"""
Tiny DAG — understand the basics
=================================
Two tasks:
  1. hello  — PythonOperator (runs in scheduler, no pod)
  2. spark  — KubernetesPodOperator (pulls charmed-spark, runs spark8t)

Where does spark8t come from?
  The charmed-spark image has spark8t pre-installed.
  spark8t reads S3 + cluster config from the SA's companion K8s Secret
  (spark8t-sa-conf-<username>), so there are no hardcoded credentials here.

Works on BOTH KubernetesExecutor and LocalExecutor via the same os.environ pattern:
  KubernetesExecutor: Coordinator patches the worker pod template → env vars injected per pod.
  LocalExecutor:      Coordinator adds SPARK_NAMESPACE/USERNAME to the scheduler Pebble plan
                      → scheduler process has them → LocalExecutor subprocesses inherit them.
"""
from __future__ import annotations

import os
from datetime import timedelta

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Same pattern for both executors:
#   KubernetesExecutor — Coordinator patches the worker pod template with these env vars.
#   LocalExecutor      — Coordinator adds them to the scheduler Pebble plan;
#                        LocalExecutor tasks are subprocesses of the scheduler and inherit its env.
SA        = os.environ.get("SPARK_USERNAME")
NAMESPACE = os.environ.get("SPARK_NAMESPACE")

SPARK_IMAGE = "ghcr.io/canonical/charmed-spark:3.5-22.04_edge"

def hello():
    print("Hello from Airflow!")
    print(f"  SA={SA}  NAMESPACE={NAMESPACE}")


SPARK_SCRIPT = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TinyDemo").getOrCreate()

data = [("Alice", 1), ("Bob", 2), ("Carol", 3)]
df = spark.createDataFrame(data, ["name", "value"])
df.show()

print("Total rows:", df.count())
spark.stop()
"""

SPARK_COMMAND = (
    f"set -e\n"
    f"cat > /tmp/job.py << 'PYSCRIPT'\n"
    f"{SPARK_SCRIPT}\n"
    f"PYSCRIPT\n"
    f"python3 -m spark8t.cli.spark_submit "
    f"--username {SA} --namespace {NAMESPACE} "
    f"--deploy-mode client "
    f"--conf spark.app.name=TinyDemo "
    f"/tmp/job.py\n"
)

with DAG(
    dag_id="tiny_spark_demo",
    schedule=None,
    catchup=False,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=10)},
    tags=["demo", "spark8t", "tiny"],
    description="Minimal 2-task DAG: Python task → Spark job via spark8t",
) as dag:

    hello_task = PythonOperator(
        task_id="hello",
        python_callable=hello,
    )

    spark_task = KubernetesPodOperator(
        task_id="spark_job",
        name="tiny-spark-job",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        cmds=["/bin/bash", "-c"],
        arguments=[SPARK_COMMAND],
        service_account_name=SA,
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "200m", "memory": "512Mi"},
            limits={"cpu": "500m", "memory": "1Gi"},
        ),
        is_delete_operator_pod=False,
        get_logs=True,
        startup_timeout_seconds=120,
        log_events_on_failure=True,
    )

    hello_task >> spark_task
