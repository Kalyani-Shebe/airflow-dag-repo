from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import json
import logging
import os
import queue as stdlib_queue
import time
import requests
import pandas as pd
import psycopg2
from kombu import Connection

# ------------------------------------------------------------
# DataHub lineage is optional. If the datahub-airflow-plugin
# package isn't installed in this environment, don't let that
# break DAG parsing for everyone -- just disable lineage.
# Install with: pip install acryl-datahub-airflow-plugin
# ------------------------------------------------------------
try:
    from datahub_provider.entities import Dataset as DataHubDataset
    DATAHUB_AVAILABLE = True
except ModuleNotFoundError:
    DataHubDataset = None
    DATAHUB_AVAILABLE = False
    logging.warning(
        "datahub_airflow_plugin is not installed; "
        "DataHub lineage (inlets/outlets) will be skipped for this DAG. "
        "Install 'acryl-datahub-airflow-plugin' to re-enable it."
    )


# ============================================================
# Configuration
# ============================================================

# -----------------------------
# RabbitMQ
# -----------------------------

RABBITMQ_HOST = os.getenv(
    "RABBITMQ_HOST",
    "rabbitmq.rabbitmq-tenant.svc.cluster.local",
)

RABBITMQ_PORT = int(
    os.getenv("RABBITMQ_PORT", "5672")
)

RABBITMQ_USER = os.getenv(
    "RABBITMQ_USER",
    "rmq_user",
)

RABBITMQ_PASSWORD = os.getenv(
    "RABBITMQ_PASSWORD",
    "",
)

RABBITMQ_QUEUE = os.getenv(
    "RABBITMQ_QUEUE",
    "orders.raw",
)


# -----------------------------
# Pipeline storage
# -----------------------------

PIPELINE_DATA_DIR = os.getenv(
    "PIPELINE_DATA_DIR",
    "/opt/airflow/staging",
)


# -----------------------------
# PostgreSQL
# -----------------------------

POSTGRES_HOST = os.getenv(
    "MY_POSTGRES_HOST",
    "pg-primary.data-platform.svc.cluster.local",
)

POSTGRES_PORT = int(
    os.getenv("MY_POSTGRES_PORT", "5432")
)

POSTGRES_DB = os.getenv(
    "MY_POSTGRES_DB",
    "data_warehouse",
)

POSTGRES_USER = os.getenv(
    "MY_POSTGRES_USER",
    "postgres",
)

POSTGRES_PASSWORD = os.getenv(
    "MY_POSTGRES_PASSWORD",
    "",
)


# -----------------------------
# DataHub
# -----------------------------

DATAHUB_ENV = os.getenv(
    "DATAHUB_ENV",
    "PROD",
)

# -----------------------------
# SeaTunnel
# -----------------------------

SEATUNNEL_REST_URL = os.getenv(
    "SEATUNNEL_REST_URL",
    "http://seatunnel.data-platform.svc.cluster.local:5801",
)


# -----------------------------
# Runtime
# -----------------------------

MAX_MESSAGES = int(
    os.getenv("MAX_MESSAGES", "100000")
)


# ============================================================
# Helpers
# ============================================================

def ensure_data_dir():
    """Create and return the pipeline staging directory."""

    os.makedirs(
        PIPELINE_DATA_DIR,
        exist_ok=True,
    )

    if not os.access(
        PIPELINE_DATA_DIR,
        os.W_OK,
    ):
        raise PermissionError(
            f"Pipeline directory is not writable: "
            f"{PIPELINE_DATA_DIR}"
        )

    logging.info(
        "Using pipeline storage directory: %s",
        PIPELINE_DATA_DIR,
    )

    return PIPELINE_DATA_DIR


def get_rabbitmq_connection():
    return Connection(
        hostname=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        userid=RABBITMQ_USER,
        password=RABBITMQ_PASSWORD,
        transport="pyamqp",
    )


def get_postgres_connection():
    if not POSTGRES_PASSWORD:
        raise ValueError(
            "POSTGRES_PASSWORD is not configured. "
            "Configure it in the Airflow worker environment "
            "before running the PostgreSQL task."
        )

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10,
    )


def make_dataset(platform, name, env):
    """Return a DataHub Dataset entity, or None if the plugin isn't installed."""
    if not DATAHUB_AVAILABLE:
        return None
    return DataHubDataset(platform=platform, name=name, env=env)


# ============================================================
# DAG
# ============================================================

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="rabbitmq_to_multi_store_pipeline",
    default_args=default_args,
    description=(
        "RabbitMQ -> Python/Pandas -> PostgreSQL "
        "-> local Airflow staging, with DataHub lineage"
    ),
    schedule="0 * * * *",
    catchup=False,
    tags=[
        "production",
        "rabbitmq",
        "python",
        "pandas",
        "postgres",
        "staging",
        "datahub",
    ],
) as dag:

    # ========================================================
    # TASK 0
    # Publish dummy messages to RabbitMQ
    # ========================================================

    def publish_dummy_messages():

        logging.info(
            "Publishing dummy messages to RabbitMQ queue: %s",
            RABBITMQ_QUEUE,
        )

        dummy_messages = [
            {
                "order_id": "ORD-1001",
                "customer_id": "CUST-001",
                "amount": 1250.50,
                "order_date": "2026-08-10",
            },
            {
                "order_id": "ORD-1002",
                "customer_id": "CUST-002",
                "amount": 875.25,
                "order_date": "2026-08-10",
            },
            {
                "order_id": "ORD-1003",
                "customer_id": "CUST-003",
                "amount": 2340.00,
                "order_date": "2026-08-10",
            },
            {
                "order_id": "ORD-1004",
                "customer_id": "CUST-004",
                "amount": 450.75,
                "order_date": "2026-08-10",
            },
            {
                "order_id": "ORD-1005",
                "customer_id": "CUST-005",
                "amount": 1999.99,
                "order_date": "2026-08-10",
            },
        ]

        with get_rabbitmq_connection() as conn:

            conn.ensure_connection(
                max_retries=3
            )

            simple_queue = conn.SimpleQueue(
                RABBITMQ_QUEUE
            )

            try:

                for message in dummy_messages:

                    simple_queue.put(
                        json.dumps(message)
                    )

                    logging.info(
                        "Published message: %s",
                        message,
                    )

            finally:

                simple_queue.close()

        logging.info(
            "Successfully published %d dummy messages",
            len(dummy_messages),
        )

        return True


    task_publish_dummy_messages = PythonOperator(
        task_id="publish_dummy_messages",
        python_callable=publish_dummy_messages,
    )


    # ========================================================
    # TASK 1
    # Validate RabbitMQ
    # ========================================================

    def validate_rabbitmq():

        logging.info(
            "Checking RabbitMQ connectivity..."
        )

        logging.info(
            "RabbitMQ host: %s",
            RABBITMQ_HOST,
        )

        logging.info(
            "RabbitMQ port: %s",
            RABBITMQ_PORT,
        )

        logging.info(
            "RabbitMQ user: %s",
            RABBITMQ_USER,
        )

        with get_rabbitmq_connection() as conn:

            conn.ensure_connection(
                max_retries=3
            )

            channel = conn.channel()

            result = channel.queue_declare(
                queue=RABBITMQ_QUEUE,
                passive=True,
            )

            logging.info(
                "RabbitMQ connection successful"
            )

            logging.info(
                "Queue: %s",
                RABBITMQ_QUEUE,
            )

            logging.info(
                "Messages available: %s",
                result.message_count,
            )

            channel.close()

        return True


    task_validate_rabbitmq = PythonOperator(
        task_id="validate_rabbitmq",
        python_callable=validate_rabbitmq,
    )


    # ========================================================
    # TASK 2
    # Consume RabbitMQ messages
    # ========================================================

    def consume_rabbitmq(**context):

        execution_date = context["ds"]

        data_dir = ensure_data_dir()

        output_file = os.path.join(
            data_dir,
            f"orders_raw_{execution_date}.json",
        )

        logging.info(
            "Reading RabbitMQ queue: %s",
            RABBITMQ_QUEUE,
        )

        logging.info(
            "Writing raw data to: %s",
            output_file,
        )

        messages = []

        with get_rabbitmq_connection() as conn:

            conn.ensure_connection(
                max_retries=3
            )

            simple_queue = conn.SimpleQueue(
                RABBITMQ_QUEUE
            )

            try:

                for _ in range(MAX_MESSAGES):

                    try:

                        message = simple_queue.get(
                            block=False
                        )

                    except stdlib_queue.Empty:

                        break

                    try:

                        body = message.body

                        if isinstance(body, bytes):
                            body = body.decode("utf-8")

                        if isinstance(body, str):
                            body = json.loads(body)

                        messages.append(body)

                        message.ack()

                    except Exception as exc:

                        logging.error(
                            "Invalid RabbitMQ message: %s",
                            exc,
                        )

                        message.reject(
                            requeue=False
                        )

            finally:

                simple_queue.close()

        with open(
            output_file,
            "w",
            encoding="utf-8",
        ) as f:

            for message in messages:

                f.write(
                    json.dumps(message)
                    + "\n"
                )

        logging.info(
            "Consumed %d messages",
            len(messages),
        )

        if not messages:

            logging.warning(
                "RabbitMQ queue '%s' contained no messages.",
                RABBITMQ_QUEUE,
            )

        return output_file


    _consume_inlets = []
    _rabbitmq_dataset = make_dataset(
        platform="rabbitmq",
        name=RABBITMQ_QUEUE,
        env=DATAHUB_ENV,
    )
    if _rabbitmq_dataset is not None:
        _consume_inlets.append(_rabbitmq_dataset)

    task_consume_rabbitmq = PythonOperator(
        task_id="consume_rabbitmq_messages",
        python_callable=consume_rabbitmq,
        inlets=_consume_inlets,
    )


    # ========================================================
    # TASK 3
    # Transform using Pandas
    # ========================================================

    def transform_orders(**context):

        execution_date = context["ds"]

        data_dir = ensure_data_dir()

        input_file = os.path.join(
            data_dir,
            f"orders_raw_{execution_date}.json",
        )

        output_file = os.path.join(
            data_dir,
            f"orders_enriched_{execution_date}.parquet",
        )

        logging.info(
            "Reading raw orders: %s",
            input_file,
        )

        if not os.path.exists(input_file):

            raise FileNotFoundError(
                f"Input file does not exist: "
                f"{input_file}"
            )

        try:

            df = pd.read_json(
                input_file,
                lines=True,
            )

        except ValueError:

            logging.warning(
                "No valid JSON records found."
            )

            df = pd.DataFrame()

        if df.empty:

            logging.warning(
                "No orders received for %s",
                execution_date,
            )

            df.to_parquet(
                output_file,
                index=False,
            )

            return output_file

        # -------------------------
        # Amount
        # -------------------------

        if "amount" in df.columns:

            df["amount"] = pd.to_numeric(
                df["amount"],
                errors="coerce",
            )

        # -------------------------
        # Order date
        # -------------------------

        if "order_date" in df.columns:

            df["order_date"] = pd.to_datetime(
                df["order_date"],
                errors="coerce",
            )

        # -------------------------
        # Ingestion metadata
        # -------------------------

        df["ingestion_date"] = execution_date

        df["pipeline"] = (
            "rabbitmq_to_multi_store_pipeline"
        )

        # -------------------------
        # Remove duplicates
        # -------------------------

        if "order_id" in df.columns:

            df = df.drop_duplicates(
                subset=["order_id"]
            )

        logging.info(
            "Transformed %d records",
            len(df),
        )

        # -------------------------
        # Write Parquet
        # -------------------------

        df.to_parquet(
            output_file,
            index=False,
        )

        logging.info(
            "Created parquet file: %s",
            output_file,
        )

        return output_file


    task_transform = PythonOperator(
        task_id="transform_orders_pandas",
        python_callable=transform_orders,
    )


    # ========================================================
    # TASK 4
    # Write to PostgreSQL
    # ========================================================

    def write_to_postgres(**context):

        execution_date = context["ds"]

        data_dir = ensure_data_dir()

        input_file = os.path.join(
            data_dir,
            f"orders_enriched_{execution_date}.parquet",
        )

        if not os.path.exists(input_file):

            raise FileNotFoundError(
                input_file
            )

        df = pd.read_parquet(
            input_file
        )

        if df.empty:

            logging.info(
                "No records to write to PostgreSQL."
            )

            return

        logging.info(
            "Connecting to PostgreSQL:"
        )

        logging.info(
            "Host=%s",
            POSTGRES_HOST,
        )

        logging.info(
            "Port=%s",
            POSTGRES_PORT,
        )

        logging.info(
            "Database=%s",
            POSTGRES_DB,
        )

        connection = get_postgres_connection()

        cursor = None

        try:

            cursor = connection.cursor()

            # -------------------------
            # Create destination table
            # -------------------------

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS order_summary (
                    order_id TEXT PRIMARY KEY,
                    customer_id TEXT,
                    amount NUMERIC,
                    load_date DATE
                )
                """
            )

            # -------------------------
            # Insert records
            # -------------------------

            inserted_count = 0

            for _, row in df.iterrows():

                order_id = row.get(
                    "order_id"
                )

                customer_id = row.get(
                    "customer_id"
                )

                amount = row.get(
                    "amount"
                )

                if pd.isna(order_id):

                    logging.warning(
                        "Skipping record without order_id"
                    )

                    continue

                if pd.isna(customer_id):

                    customer_id = None

                if pd.isna(amount):

                    amount = None

                cursor.execute(
                    """
                    INSERT INTO order_summary
                        (
                            order_id,
                            customer_id,
                            amount,
                            load_date
                        )
                    VALUES
                        (%s, %s, %s, %s)
                    ON CONFLICT (order_id)
                    DO NOTHING
                    """,
                    (
                        str(order_id),
                        customer_id,
                        amount,
                        execution_date,
                    ),
                )

                inserted_count += cursor.rowcount

            connection.commit()

            logging.info(
                "PostgreSQL load completed: %d records inserted",
                inserted_count,
            )

        except Exception:

            connection.rollback()

            logging.exception(
                "PostgreSQL load failed"
            )

            raise

        finally:

            if cursor is not None:
                cursor.close()

            connection.close()


    _postgres_outlets = []
    _postgres_dataset = make_dataset(
        platform="postgres",
        name=f"{POSTGRES_DB}.public.order_summary",
        env=DATAHUB_ENV,
    )
    if _postgres_dataset is not None:
        _postgres_outlets.append(_postgres_dataset)

    task_write_postgres = PythonOperator(
        task_id="write_to_postgresql",
        python_callable=write_to_postgres,
        outlets=_postgres_outlets,
    )


    # ========================================================
    # TASK 5
    # Verify staging storage
    # ========================================================

    def verify_storage(**context):

        execution_date = context["ds"]

        data_dir = ensure_data_dir()

        raw_file = os.path.join(
            data_dir,
            f"orders_raw_{execution_date}.json",
        )

        parquet_file = os.path.join(
            data_dir,
            f"orders_enriched_{execution_date}.parquet",
        )

        logging.info(
            "Verifying pipeline storage..."
        )

        logging.info(
            "Storage directory: %s",
            data_dir,
        )

        if not os.path.exists(raw_file):

            raise FileNotFoundError(
                raw_file
            )

        if not os.path.exists(parquet_file):

            raise FileNotFoundError(
                parquet_file
            )

        raw_size = os.path.getsize(
            raw_file
        )

        parquet_size = os.path.getsize(
            parquet_file
        )

        logging.info(
            "Raw file size: %d bytes",
            raw_size,
        )

        logging.info(
            "Parquet file size: %d bytes",
            parquet_size,
        )

        logging.info(
            "Storage verification successful."
        )


    # NOTE: this was previously indented *inside* verify_storage(),
    # so it never ran as a DAG-level statement and
    # `task_verify_storage` would have been undefined when the
    # dependency chain below referenced it. Moved out to fix that.
    task_verify_storage = PythonOperator(
        task_id="verify_storage",
        python_callable=verify_storage,
    )


    # ========================================================
    # TASK 6
    # Trigger SeaTunnel job
    # ========================================================

    def trigger_seatunnel_job(**context):
        job_config = {
            "env": {"job.mode": "batch"},
            "source": [
                {
                    "plugin_name": "Jdbc",
                    "result_table_name": "orders",
                    "url": f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
                    "driver": "org.postgresql.Driver",
                    "user": POSTGRES_USER,
                    "password": POSTGRES_PASSWORD,
                    "query": "SELECT * FROM order_summary",
                }
            ],
            "sink": [
                {
                    "plugin_name": "Console",
                    "source_table_name": ["orders"],
                }
            ],
        }

        resp = requests.post(
            f"{SEATUNNEL_REST_URL}/hazelcast/rest/maps/submit-job",
            json=job_config,
            params={"jobName": "airflow_triggered_order_sync"},
            timeout=30,
        )
        resp.raise_for_status()
        job_id = resp.json()["jobId"]

        logging.info("Submitted SeaTunnel job, jobId=%s", job_id)

        context["ti"].xcom_push(key="seatunnel_job_id", value=job_id)
        return job_id


    task_trigger_seatunnel = PythonOperator(
        task_id="trigger_seatunnel_job",
        python_callable=trigger_seatunnel_job,
    )


    # ========================================================
    # TASK 7
    # Verify SeaTunnel job completion
    # ========================================================

    def verify_seatunnel_job(**context):
        job_id = context["ti"].xcom_pull(
            task_ids="trigger_seatunnel_job",
            key="seatunnel_job_id",
        )

        terminal_states = {"FINISHED", "CANCELED", "FAILED", "UNKNOWABLE"}
        max_wait_seconds = 600
        poll_interval = 10
        waited = 0

        while waited < max_wait_seconds:
            resp = requests.get(
                f"{SEATUNNEL_REST_URL}/hazelcast/rest/maps/job-info/{job_id}",
                timeout=15,
            )
            resp.raise_for_status()
            info = resp.json()
            status = info.get("jobStatus")

            logging.info("SeaTunnel job %s status: %s", job_id, status)

            if status in terminal_states:
                if status != "FINISHED":
                    raise RuntimeError(
                        f"SeaTunnel job {job_id} ended with status {status}: "
                        f"{info.get('errorMsg')}"
                    )
                logging.info("SeaTunnel job %s completed successfully", job_id)
                return

            time.sleep(poll_interval)
            waited += poll_interval

        raise TimeoutError(f"SeaTunnel job {job_id} did not finish in time")


    task_verify_seatunnel = PythonOperator(
        task_id="verify_seatunnel_job",
        python_callable=verify_seatunnel_job,
    )


    # ========================================================
    # DAG DEPENDENCIES
    # ========================================================

    (
        task_publish_dummy_messages
        >> task_validate_rabbitmq
        >> task_consume_rabbitmq
        >> task_transform
        >> task_write_postgres
        >> task_verify_storage
        >> task_trigger_seatunnel
        >> task_verify_seatunnel
    )
