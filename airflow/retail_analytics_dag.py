from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from datetime import datetime

# -----------------------------------------
# DEFAULT CONFIG
# -----------------------------------------

default_args = {
    "owner": "rajesh",
    "start_date": datetime(2026, 1, 1),
    "retries": 1
}

# -----------------------------------------
# DAG
# -----------------------------------------

with DAG(

    dag_id="retail_analytics_pipeline",

    default_args=default_args,

    schedule_interval="@daily",

    catchup=False,

    tags=["gcp", "analytics", "data-engineering"]

) as dag:

    # -----------------------------------------
    # TASK 1
    # -----------------------------------------

    def validate_streaming_pipeline():

        print("Validating Pub/Sub and Dataflow pipeline...")

    validate_streaming = PythonOperator(

        task_id="validate_streaming_pipeline",

        python_callable=validate_streaming_pipeline

    )

    # -----------------------------------------
    # TASK 2
    # -----------------------------------------


    refresh_fact_orders = BigQueryInsertJobOperator(

        task_id="refresh_fact_orders",

        configuration={

            "query": {

                "query": """

                CREATE OR REPLACE TABLE retail_curated.fact_orders AS

                SELECT

                    order_id,
                    customer_id,
                    product_id,

                    category,
                    payment_method,
                    order_status,

                    price,
                    quantity,

                    ROUND(price * quantity, 2) AS total_amount,

                    DATE(order_timestamp) AS order_date,

                    EXTRACT(HOUR FROM order_timestamp) AS order_hour,

                    FORMAT_DATE('%A', DATE(order_timestamp)) AS order_day_name,

                    customer_city,
                    customer_state,
                    customer_country,

                    CURRENT_TIMESTAMP() AS ingestion_timestamp

                FROM retail_raw.orders_streaming_partitioned

                """,

                "useLegacySql": False

            }

        },

        location="asia-south1"

    )


    # -----------------------------------------
    # TASK 3
    # -----------------------------------------


    refresh_category_kpis = BigQueryInsertJobOperator(

        task_id="refresh_category_kpis",

        configuration={

            "query": {

                "query": """

                CREATE OR REPLACE TABLE retail_analytics.category_kpis AS

                SELECT

                    category,

                    COUNT(*) AS total_orders,

                    ROUND(SUM(total_amount), 2) AS total_revenue,

                    ROUND(AVG(total_amount), 2) AS avg_order_value,

                    CURRENT_TIMESTAMP() AS last_updated

                FROM retail_curated.fact_orders

                GROUP BY category

                """,

                "useLegacySql": False

            }

        },
        
        location="asia-south1"

    )

    # -----------------------------------------
    # DAG FLOW
    # -----------------------------------------

    validate_streaming >> refresh_fact_orders >> refresh_category_kpis
