"""
Parallel Spark Analytics Pipeline — KubernetesExecutor edition
===============================================================
Demonstrates KubernetesExecutor spawning MULTIPLE worker pods simultaneously,
with Spark jobs submitted via spark8t — Canonical's toolkit for running Spark
on Kubernetes.

spark8t reads S3 credentials and Spark configuration from a Kubernetes Secret
(integrator-hub-conf-<username>) managed by the Spark Integration Hub.
This eliminates hardcoded credentials in DAG code.

How SPARK_NAMESPACE and SPARK_USERNAME reach this DAG (KubernetesExecutor path):
┌─────────────────────────────────────────────────────────────────────────────┐
│  Integration Hub                                                            │
│    ↓ spark-service-account relation (dummy-spark-requirer charm)            │
│  auto-creates SA + RBAC + config Secret (integrator-hub-conf-spark)         │
│    ↓                                                                        │
│  Coordinator charm receives SA identity via sensitive_data pipeline         │
│    ↓ airflow-kubernetes-executor-config relation                            │
│  KubernetesExecutor charm injects vars into its worker pod template:        │
│      env:                                                                   │
│        - name: SPARK_NAMESPACE   value: <model-namespace>                   │
│        - name: SPARK_USERNAME    value: spark                               │
│    ↓                                                                        │
│  Every worker pod (one per task) inherits these env vars                    │
│    ↓ os.environ.get("SPARK_NAMESPACE") / os.environ.get("SPARK_USERNAME")   │
│  KubernetesPodOperator(namespace=NAMESPACE, service_account_name=SA, ...)   │
│    ↓                                                                        │
│  Spark client/driver runs in that pod; Spark creates executor pods in       │
│  NAMESPACE with the SA's RBAC                                               │
│  spark8t reads integrator-hub-conf-spark Secret → S3 + cluster config      │
└─────────────────────────────────────────────────────────────────────────────┘

Demo setup: patch_spark_env_from_hub.py simulates the Coordinator step by
reading the Hub Secret and patching the rendered worker pod template directly.

Compare with dag_local_executor.py (LocalExecutor path):
  - Here: NAMESPACE/SA resolved at task-run time from os.environ (worker pod env)
  - There: NAMESPACE/SA resolved at DAG-parse time from Airflow Variables (DB)

Structure (5 tasks, 3 running concurrently):
    ingest_data (1 pod)
        |
        +---> analyze_categories (pod 2)  \\
        +---> analyze_regions (pod 3)      |-- PARALLEL
        +---> analyze_trends (pod 4)      /
        |
    executive_summary (1 pod, waits for all 3)
"""
from __future__ import annotations
import os
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import timedelta

SPARK_IMAGE = "ghcr.io/canonical/charmed-spark:3.5-22.04_edge"

# Read SA coordinates from env vars injected by the KubernetesExecutor worker
# pod template.  This is the KubernetesExecutor path: every worker pod that
# runs a DAG task has SPARK_NAMESPACE and SPARK_USERNAME in its environment
# because the KubernetesExecutor charm patches the pod template after receiving
# these values from Coordinator via the sensitive_data pipeline.
#
# For the demo, patch_spark_env_from_hub.py simulates this by reading the
# Integration Hub's SA config Secret and updating the rendered pod template.
#
# Compare: dag_local_executor.py reads these from Airflow Variables instead,
# because LocalExecutor tasks run inside the scheduler container (not pods).
SA = os.environ.get("SPARK_USERNAME", "spark")
NAMESPACE = os.environ.get("SPARK_NAMESPACE", "airflow-spark")  # model namespace

LIGHT = k8s.V1ResourceRequirements(
    requests={"cpu": "200m", "memory": "256Mi"},
    limits={"cpu": "500m", "memory": "512Mi"},
)
HEAVY = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"},
    limits={"cpu": "1", "memory": "2Gi"},
)

# ---------------------------------------------------------------------------
# spark8t bootstrap: the Charmed Spark image has spark8t pre-installed.
# spark8t reads Spark + S3 config from the SA's companion Secret
# (spark8t-sa-conf-spark), so DAGs never contain credentials.
# ---------------------------------------------------------------------------
def spark8t_submit(script_body: str, app_name: str = "spark-job") -> str:
    """Wrap a PySpark script for submission via spark8t.

    1. Write the PySpark script to /tmp/job.py
    2. Invoke spark8t's spark-submit wrapper, which:
       - reads config from the SA Secret (S3 creds, image, executor settings)
       - calls $SPARK_HOME/bin/spark-submit with --master k8s://... --properties-file ...
    """
    # Determine namespace arg — if NAMESPACE is set, use it; otherwise omit
    ns_arg = f"--namespace {NAMESPACE}" if NAMESPACE else ""
    return (
        f"set -e\n"
        f"cat > /tmp/job.py << 'PYSCRIPT'\n"
        f"{script_body}\n"
        f"PYSCRIPT\n"
        f"python3 -m spark8t.cli.spark_submit "
        f"--username {SA} {ns_arg} "
        f"--deploy-mode client "
        f"--conf spark.app.name={app_name} "
        f"/tmp/job.py\n"
    )

# ---------------------------------------------------------------------------
# Task 1: Ingest — generate 50K sales records, upload to MicroCeph S3
# This task does NOT use Spark — it's pure Python with direct S3 upload.
# S3 details come from env vars injected by the justfile's configure-spark.
# ---------------------------------------------------------------------------
INGEST_SCRIPT = r'''
python3 << 'PYSCRIPT'
import csv, io, random, datetime, hashlib, hmac, base64, http.client, time, os
from email.utils import formatdate

print("=" * 72)
print("  TASK 1: DATA INGESTION")
print("=" * 72)
print("  Generating 50,000 sales records...")
time.sleep(5)

random.seed(2026)
CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports",
               "Books", "Automotive", "Health", "Food & Beverage",
               "Toys", "Beauty", "Pet Supplies", "Office"]
REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America",
           "Middle East", "Africa", "Oceania"]
CHANNELS = ["Online", "Retail", "Wholesale", "Marketplace", "Direct"]

rows = []
start = datetime.date(2025, 1, 1)
for i in range(50000):
    day_offset = random.randint(0, 364)
    date = start + datetime.timedelta(days=day_offset)
    cat = random.choice(CATEGORIES)
    region = random.choice(REGIONS)
    channel = random.choice(CHANNELS)
    quantity = random.randint(1, 100)
    base_price = {"Electronics": 350, "Clothing": 65, "Home & Garden": 95,
                  "Sports": 140, "Books": 22, "Automotive": 280,
                  "Health": 45, "Food & Beverage": 28, "Toys": 55,
                  "Beauty": 75, "Pet Supplies": 42, "Office": 38}[cat]
    price = round(base_price * random.uniform(0.5, 2.0), 2)
    revenue = round(price * quantity, 2)
    rows.append([i+1, date.isoformat(), cat, region, channel, quantity, price, revenue])

buf = io.StringIO()
w = csv.writer(buf)
w.writerow(["order_id","date","category","region","channel","quantity","unit_price","revenue"])
w.writerows(rows)
body = buf.getvalue().encode()

ENDPOINT = os.environ.get("S3_HOST", "10.0.0.78")
PORT = int(os.environ.get("S3_PORT", "80"))
BUCKET = os.environ.get("S3_BUCKET", "airflow-spark")
KEY = "demo/sales_50k.csv"
ACCESS = os.environ.get("S3_ACCESS", "airflow-access-key")
SECRET = os.environ.get("S3_SECRET", "airflow-secret-key")

date_str = formatdate(usegmt=True)
sts = "PUT\n\ntext/csv\n" + date_str + "\n/" + BUCKET + "/" + KEY
signature = base64.b64encode(hmac.new(SECRET.encode(), sts.encode(), hashlib.sha1).digest()).decode()

conn = http.client.HTTPConnection(ENDPOINT, PORT)
headers = {"Content-Type": "text/csv", "Date": date_str, "Authorization": "AWS " + ACCESS + ":" + signature}
conn.request("PUT", "/" + BUCKET + "/" + KEY, body=body, headers=headers)
resp = conn.getresponse()
if resp.status not in (200, 204):
    raise Exception("Upload failed: " + str(resp.status))

print("  Records generated : 50,000")
print("  Categories        : " + str(len(CATEGORIES)))
print("  Regions           : " + str(len(REGIONS)))
print("  Channels          : " + str(len(CHANNELS)))
print("  Uploaded to       : s3a://airflow-spark/demo/sales_50k.csv")
print("  Size              : " + str(len(body) // 1024) + " KB")
print("=" * 72)
PYSCRIPT
'''

# ---------------------------------------------------------------------------
# Task 2a: Category analysis (runs in PARALLEL with 2b and 2c)
# No hardcoded S3 config — spark8t injects it from the SA Secret.
# ---------------------------------------------------------------------------
CATEGORIES_JOB = r'''
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

print("=" * 72)
print("  TASK 2a: CATEGORY DEEP-DIVE (running in parallel)")
print("=" * 72)

spark = SparkSession.builder.appName("CategoryAnalysis").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("s3a://airflow-spark/demo/sales_50k.csv")
df.cache()
print("  Loaded " + str(df.count()) + " records")

time.sleep(10)

cat_stats = (df.groupBy("category").agg(
    F.sum("revenue").alias("total_revenue"),
    F.count("*").alias("orders"),
    F.avg("unit_price").alias("avg_price"),
    F.avg("quantity").alias("avg_qty"),
    F.stddev("revenue").alias("revenue_stddev")
).orderBy(F.desc("total_revenue")).collect())

max_rev = max(r["total_revenue"] for r in cat_stats)
print("\n  CATEGORY PERFORMANCE RANKING")
print("  " + "-" * 65)
for r in cat_stats:
    bar_len = int(25 * r["total_revenue"] / max_rev)
    bar = "#" * bar_len
    print("  %-16s $%12s  orders: %5d  avg_qty: %4.0f  %s" % (
        r["category"], "{:,.0f}".format(r["total_revenue"]),
        r["orders"], r["avg_qty"], bar))

result = df.groupBy("category").agg(
    F.sum("revenue").alias("total_revenue"),
    F.count("*").alias("orders"),
    F.avg("unit_price").alias("avg_price"))
result.coalesce(1).write.mode("overwrite").parquet("s3a://airflow-spark/demo/results/categories")

print("\n  Saved to: s3a://airflow-spark/demo/results/categories/")
print("=" * 72)
spark.stop()
'''
CATEGORIES_SCRIPT = spark8t_submit(CATEGORIES_JOB, "CategoryAnalysis")

# ---------------------------------------------------------------------------
# Task 2b: Regional analysis (runs in PARALLEL)
# ---------------------------------------------------------------------------
REGIONS_JOB = r'''
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

print("=" * 72)
print("  TASK 2b: REGIONAL BREAKDOWN (running in parallel)")
print("=" * 72)

spark = SparkSession.builder.appName("RegionalAnalysis").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("s3a://airflow-spark/demo/sales_50k.csv")
df.cache()
print("  Loaded " + str(df.count()) + " records")

time.sleep(12)

reg_stats = (df.groupBy("region").agg(
    F.sum("revenue").alias("total_revenue"),
    F.count("*").alias("orders"),
    F.countDistinct("category").alias("categories_sold"),
    F.avg("revenue").alias("avg_order_value")
).orderBy(F.desc("total_revenue")).collect())

max_rev = max(r["total_revenue"] for r in reg_stats)
print("\n  REGIONAL REVENUE MAP")
print("  " + "-" * 65)
for r in reg_stats:
    bar_len = int(25 * r["total_revenue"] / max_rev)
    bar = "=" * bar_len
    print("  %-16s $%12s  orders: %5d  cats: %2d  avg: $%8s  %s" % (
        r["region"], "{:,.0f}".format(r["total_revenue"]),
        r["orders"], r["categories_sold"],
        "{:,.0f}".format(r["avg_order_value"]), bar))

cross = (df.groupBy("region", "channel").agg(
    F.sum("revenue").alias("revenue")
).orderBy("region", F.desc("revenue")))
cross.coalesce(1).write.mode("overwrite").parquet("s3a://airflow-spark/demo/results/regions")

print("\n  Saved to: s3a://airflow-spark/demo/results/regions/")
print("=" * 72)
spark.stop()
'''
REGIONS_SCRIPT = spark8t_submit(REGIONS_JOB, "RegionalAnalysis")

# ---------------------------------------------------------------------------
# Task 2c: Time-series trend analysis (runs in PARALLEL)
# ---------------------------------------------------------------------------
TRENDS_JOB = r'''
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("=" * 72)
print("  TASK 2c: TIME-SERIES TRENDS (running in parallel)")
print("=" * 72)

spark = SparkSession.builder.appName("TrendAnalysis").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("s3a://airflow-spark/demo/sales_50k.csv")
df.cache()
print("  Loaded " + str(df.count()) + " records")

time.sleep(15)

monthly = (df.withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMM"))
    .groupBy("month", "month_name").agg(
        F.sum("revenue").alias("revenue"),
        F.count("*").alias("orders"),
        F.avg("quantity").alias("avg_qty")
    ).orderBy("month"))

w = Window.orderBy("month")
monthly_growth = monthly.withColumn("prev_revenue", F.lag("revenue").over(w))
monthly_growth = monthly_growth.withColumn("growth_pct",
    F.round((F.col("revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100, 1))

rows = monthly_growth.collect()
max_rev = max(r["revenue"] for r in rows)
print("\n  MONTHLY REVENUE + GROWTH")
print("  " + "-" * 65)
for r in rows:
    bar_len = int(25 * r["revenue"] / max_rev)
    bar = "*" * bar_len
    growth = "  N/A" if r["growth_pct"] is None else "%+5.1f%%" % r["growth_pct"]
    print("  %-4s $%12s  orders: %5d  growth: %s  %s" % (
        r["month_name"], "{:,.0f}".format(r["revenue"]),
        r["orders"], growth, bar))

weekly = (df.withColumn("dow", F.dayofweek("date"))
    .groupBy("dow").agg(F.sum("revenue").alias("revenue"))
    .orderBy("dow").collect())
days = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
max_w = max(r["revenue"] for r in weekly)
print("\n  WEEKLY PATTERN")
print("  " + "-" * 40)
for r in weekly:
    bar_len = int(20 * r["revenue"] / max_w)
    bar = "+" * bar_len
    print("  %-4s $%12s  %s" % (days[r["dow"]-1], "{:,.0f}".format(r["revenue"]), bar))

monthly.coalesce(1).write.mode("overwrite").parquet("s3a://airflow-spark/demo/results/trends")

print("\n  Saved to: s3a://airflow-spark/demo/results/trends/")
print("=" * 72)
spark.stop()
'''
TRENDS_SCRIPT = spark8t_submit(TRENDS_JOB, "TrendAnalysis")

# ---------------------------------------------------------------------------
# Task 3: Executive summary (reads all results)
# ---------------------------------------------------------------------------
SUMMARY_JOB = r'''
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("ExecutiveSummary").getOrCreate()

time.sleep(5)

cats = spark.read.parquet("s3a://airflow-spark/demo/results/categories")
regs = spark.read.parquet("s3a://airflow-spark/demo/results/regions")
trends = spark.read.parquet("s3a://airflow-spark/demo/results/trends")

total_revenue = cats.agg(F.sum("total_revenue")).collect()[0][0]
total_orders = cats.agg(F.sum("orders")).collect()[0][0]
top_cat = cats.orderBy(F.desc("total_revenue")).first()
top_reg = regs.groupBy("region").agg(F.sum("revenue").alias("rev")).orderBy(F.desc("rev")).first()
best_month = trends.orderBy(F.desc("revenue")).first()

print()
print("+" + "=" * 70 + "+")
print("|" + "  EXECUTIVE SUMMARY - SALES ANALYTICS PIPELINE".center(70) + "|")
print("+" + "=" * 70 + "+")
print("|" + "".center(70) + "|")
print("|  Total Revenue       : $%s" % "{:>15,.0f}".format(total_revenue) + " " * 30 + "|")
print("|  Total Orders        : %s" % "{:>15,}".format(total_orders) + " " * 30 + "|")
print("|  Avg Order Value     : $%s" % "{:>15,.2f}".format(total_revenue/total_orders) + " " * 30 + "|")
print("|" + "".center(70) + "|")
print("+" + "-" * 70 + "+")
print("|  Top Category : %-20s ($%s)" % (top_cat["category"], "{:,.0f}".format(top_cat["total_revenue"])) + " " * 15 + "|")
print("|  Top Region   : %-20s ($%s)" % (top_reg["region"], "{:,.0f}".format(top_reg["rev"])) + " " * 15 + "|")
print("|  Best Month   : %-20s ($%s)" % (best_month["month_name"], "{:,.0f}".format(best_month["revenue"])) + " " * 15 + "|")
print("+" + "-" * 70 + "+")
print("|" + "".center(70) + "|")
print("|  Pipeline: 5 tasks, 3 parallel Spark jobs, 50K records processed" + " " * 3 + "|")
print("|  Submitted via spark8t - config from K8s SA Secret, zero creds" + " " * 5 + "|")
print("|" + "".center(70) + "|")
print("+" + "=" * 70 + "+")
print()
print("Pipeline complete!")

spark.stop()
'''
SUMMARY_SCRIPT = spark8t_submit(SUMMARY_JOB, "ExecutiveSummary")

# ===========================================================================
# DAG Definition
# ===========================================================================
with DAG(
    dag_id="parallel_spark_analytics",
    schedule=None,
    catchup=False,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=15)},
    tags=["spark", "parallel", "demo", "spark8t", "kubernetes-executor"],
    description=(
        "KubernetesExecutor: parallel Spark via spark8t — SPARK_NAMESPACE/USERNAME "
        "injected into worker pods by executor charm pod template"
    ),
) as dag:

    ingest = KubernetesPodOperator(
        task_id="ingest_data",
        name="ingest-data",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        cmds=["/bin/bash", "-c"],
        arguments=[INGEST_SCRIPT],
        service_account_name=SA,
        container_resources=LIGHT,
        is_delete_operator_pod=False,
        get_logs=True,
        startup_timeout_seconds=120,
        log_events_on_failure=True,
    )

    categories = KubernetesPodOperator(
        task_id="analyze_categories",
        name="analyze-categories",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        cmds=["/bin/bash", "-c"],
        arguments=[CATEGORIES_SCRIPT],
        service_account_name=SA,
        container_resources=HEAVY,
        is_delete_operator_pod=False,
        get_logs=True,
        startup_timeout_seconds=120,
        log_events_on_failure=True,
    )

    regions = KubernetesPodOperator(
        task_id="analyze_regions",
        name="analyze-regions",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        cmds=["/bin/bash", "-c"],
        arguments=[REGIONS_SCRIPT],
        service_account_name=SA,
        container_resources=HEAVY,
        is_delete_operator_pod=False,
        get_logs=True,
        startup_timeout_seconds=120,
        log_events_on_failure=True,
    )

    trends = KubernetesPodOperator(
        task_id="analyze_trends",
        name="analyze-trends",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        cmds=["/bin/bash", "-c"],
        arguments=[TRENDS_SCRIPT],
        service_account_name=SA,
        container_resources=HEAVY,
        is_delete_operator_pod=False,
        get_logs=True,
        startup_timeout_seconds=120,
        log_events_on_failure=True,
    )

    summary = KubernetesPodOperator(
        task_id="executive_summary",
        name="executive-summary",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        cmds=["/bin/bash", "-c"],
        arguments=[SUMMARY_SCRIPT],
        service_account_name=SA,
        container_resources=HEAVY,
        is_delete_operator_pod=False,
        get_logs=True,
        startup_timeout_seconds=120,
        log_events_on_failure=True,
    )

    # Fan-out / fan-in pattern: 3 tasks run in PARALLEL after ingest
    ingest >> [categories, regions, trends] >> summary
