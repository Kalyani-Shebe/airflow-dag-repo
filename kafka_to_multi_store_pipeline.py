from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import timedelta
import os
import json
import logging

import pandas as pd
import pika
import psycopg2
import requests


# ============================================================
# Configuration
# ============================================================

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

# RabbitMQ queue replacing Kafka topic orders.raw
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "orders.raw")

# PVC mount path
DATA_DIR = os.getenv("PIPELINE_DATA_DIR", "/data/staging")

# PostgreSQL
POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "pg-primary.pg-ha-tenant-a.svc.cluster.local"
)
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "data_warehouse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "dwh_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# DataHub
DATAHUB_URL = os.getenv(
    "DATAHUB_URL",
    "http://datahub:9002"
)

MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "100000"))


# ============================================================
# DAG configuration
# ============================================================

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="rabbitmq_to_multi_store_pipeline",
    default_args=default_args,
    description="RabbitMQ -> Python/Pandas -> PostgreSQL/PVC -> DataHub",
    schedule="0 * * * *",
    catchup=False,
    tags=[
        "production",
        "rabbitmq",
        "python",
        "pandas",
        "postgres",
        "pvc",
    ],
) as dag:

    # ========================================================
    # Task 1 - Validate RabbitMQ
    # ========================================================

    def validate_rabbitmq():

        logging.info("Checking RabbitMQ connectivity")

        credentials = pika.PlainCredentials(
            RABBITMQ_USER,
            RABBITMQ_PASSWORD,
        )

        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            connection_attempts=3,
            retry_delay=2,
        )

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Check that the queue exists
        result = channel.queue_declare(
            queue=RABBITMQ_QUEUE,
            passive=True,
        )

        logging.info(
            "RabbitMQ connected successfully. "
            "Queue=%s Messages=%s",
            RABBITMQ_QUEUE,
            result.method.message_count,
        )

        connection.close()

        return True


    validate_rabbitmq = PythonOperator(
        task_id="validate_rabbitmq",
        python_callable=validate_rabbitmq,
    )


    # ========================================================
    # Task 2 - Consume RabbitMQ messages
    # ========================================================

    def consume_rabbitmq(**context):

        execution_date = context["ds"]

        os.makedirs(DATA_DIR, exist_ok=True)

        output_file = os.path.join(
            DATA_DIR,
            f"orders_raw_{execution_date}.json",
        )

        logging.info(
            "Consuming RabbitMQ messages into %s",
            output_file,
        )

        credentials = pika.PlainCredentials(
            RABBITMQ_USER,
            RABBITMQ_PASSWORD,
        )

        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            connection_attempts=3,
            retry_delay=2,
        )

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        messages = []

        for _ in range(MAX_MESSAGES):

            method_frame, header_frame, body = channel.basic_get(
                queue=RABBITMQ_QUEUE,
                auto_ack=False,
            )

            if method_frame is None:
                break

            try:
                message = json.loads(body.decode("utf-8"))

                messages.append(message)

                # Acknowledge successfully processed message
                channel.basic_ack(
                    delivery_tag=method_frame.delivery_tag
                )

            except Exception as exc:

                logging.error(
                    "Failed to process RabbitMQ message: %s",
                    exc,
                )

                # Reject bad message
                channel.basic_nack(
                    delivery_tag=method_frame.delivery_tag,
                    requeue=False,
                )

        connection.close()

        with open(output_file, "w") as f:

            for message in messages:
                f.write(
                    json.dumps(message) + "\n"
                )

        logging.info(
            "Consumed %d messages",
            len(messages),
        )

        return output_file


    consume_rabbitmq = PythonOperator(
        task_id="consume_rabbitmq_messages",
        python_callable=consume_rabbitmq,
    )


    # ========================================================
    # Task 3 - Transform using Pandas
    # ========================================================

    def transform_orders(**context):

        execution_date = context["ds"]

        input_file = os.path.join(
            DATA_DIR,
            f"orders_raw_{execution_date}.json",
        )

        output_file = os.path.join(
            DATA_DIR,
            f"orders_enriched_{execution_date}.parquet",
        )

        logging.info(
            "Reading %s",
            input_file,
        )

        if not os.path.exists(input_file):

            raise FileNotFoundError(
                f"Input file does not exist: {input_file}"
            )

        # Read JSON lines
        try:

            df = pd.read_json(
                input_file,
                lines=True,
            )

        except ValueError:

            logging.warning(
                "No valid JSON records found"
            )

            df = pd.DataFrame()


        if df.empty:

            logging.warning(
                "No orders received for %s",
                execution_date,
            )

            # Create empty output
            df.to_parquet(
                output_file,
                index=False,
            )

            return output_file


        # ----------------------------------------------------
        # Basic enrichment
        # ----------------------------------------------------

        if "amount" in df.columns:

            df["amount"] = pd.to_numeric(
                df["amount"],
                errors="coerce",
            )

        if "order_date" in df.columns:

            df["order_date"] = pd.to_datetime(
                df["order_date"],
                errors="coerce",
            )

        df["ingestion_date"] = execution_date

        df["pipeline"] = (
            "rabbitmq_to_multi_store_pipeline"
        )

        # Remove duplicate orders
        if "order_id" in df.columns:

            df = df.drop_duplicates(
                subset=["order_id"]
            )

        logging.info(
            "Transformed %d records",
            len(df),
        )

        # Save to PVC
        df.to_parquet(
            output_file,
            index=False,
        )

        logging.info(
            "Created %s",
            output_file,
        )

        return output_file


    transform_orders = PythonOperator(
        task_id="transform_orders_pandas",
        python_callable=transform_orders,
    )


    # ========================================================
    # Task 4 - Write to PostgreSQL
    # ========================================================

    def write_to_postgres(**context):

        execution_date = context["ds"]

        input_file = os.path.join(
            DATA_DIR,
            f"orders_enriched_{execution_date}.parquet",
        )

        if not os.path.exists(input_file):

            raise FileNotFoundError(
                input_file
            )

        df = pd.read_parquet(input_file)

        if df.empty:

            logging.info(
                "No records to write to PostgreSQL"
            )

            return

        logging.info(
            "Connecting to PostgreSQL %s:%s",
            POSTGRES_HOST,
            POSTGRES_PORT,
        )

        connection = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )

        cursor = connection.cursor()

        # ----------------------------------------------------
        # Create destination table if it does not exist
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Insert records
        # ----------------------------------------------------

        for _, row in df.iterrows():

            order_id = row.get("order_id")
            customer_id = row.get("customer_id")
            amount = row.get("amount")

            cursor.execute(
                """
                INSERT INTO order_summary
                (
                    order_id,
                    customer_id,
                    amount,
                    load_date
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (order_id)
                DO NOTHING
                """,
                (
                    str(order_id),
                    str(customer_id),
                    amount,
                    execution_date,
                ),
            )

        connection.commit()

        cursor.close()
        connection.close()

        logging.info(
            "PostgreSQL load completed: %d records",
            len(df),
        )


    write_to_postgres = PythonOperator(
        task_id="write_to_postgresql",
        python_callable=write_to_postgres,
    )


    # ========================================================
    # Task 5 - Verify PVC output
    # ========================================================

    def verify_pvc(**context):

        execution_date = context["ds"]

        raw_file = os.path.join(
            DATA_DIR,
            f"orders_raw_{execution_date}.json",
        )

        parquet_file = os.path.join(
            DATA_DIR,
            f"orders_enriched_{execution_date}.parquet",
        )

        logging.info(
            "Checking PVC storage"
        )

        logging.info(
            "Raw file: %s",
            raw_file,
        )

        logging.info(
            "Parquet file: %s",
            parquet_file,
        )

        if not os.path.exists(raw_file):
            raise FileNotFoundError(raw_file)

        if not os.path.exists(parquet_file):
            raise FileNotFoundError(parquet_file)

        logging.info(
            "PVC verification successful"
        )

        logging.info(
            "Files in %s:",
            DATA_DIR,
        )

        for filename in os.listdir(DATA_DIR):

            logging.info(
                " - %s",
                filename,
            )


    verify_pvc = PythonOperator(
        task_id="verify_pvc_storage",
        python_callable=verify_pvc,
    )


    # ========================================================
    # Task 6 - Register lineage
    # ========================================================

    def register_lineage(**context):

        execution_date = context["ds"]

        lineage_event = {

            "pipeline":
                "rabbitmq_to_multi_store_pipeline",

            "run_date":
                execution_date,

            "upstream":
                "rabbitmq.orders.raw",

            "transform":
                "python.pandas",

            "downstream": [
                "postgres.order_summary",
                "pvc.orders_enriched",
            ],
        }

        logging.info(
            "Lineage event:"
        )

        logging.info(
            json.dumps(
                lineage_event,
                indent=2,
            )
        )

        # ----------------------------------------------------
        # Optional DataHub health check
        # ----------------------------------------------------

        try:

            response = requests.get(
                f"{DATAHUB_URL}/health",
                timeout=10,
            )

            logging.info(
                "DataHub response: %s",
                response.status_code,
            )

        except Exception as exc:

            logging.warning(
                "DataHub health check failed: %s",
                exc,
            )

        return True


    register_lineage = PythonOperator(
        task_id="register_lineage_datahub",
        python_callable=register_lineage,
    )


    # ========================================================
    # DAG dependency
    # ========================================================

    (
        validate_rabbitmq
        >> consume_rabbitmq
        >> transform_orders
        >> write_to_postgres
        >> verify_pvc
        >> register_lineage
    )
