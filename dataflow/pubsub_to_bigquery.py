
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import logging

# -----------------------------------------
# CONFIGURATION
# -----------------------------------------

PROJECT_ID = "top-vial-497707-r1"

SUBSCRIPTION = (
    f"projects/{PROJECT_ID}/subscriptions/orders-sub"
)

TABLE_SPEC = (
    f"{PROJECT_ID}:retail_raw.orders_streaming_partitioned"
)

# -----------------------------------------
# TRANSFORM FUNCTION
# -----------------------------------------

class ParseMessage(beam.DoFn):

    def process(self, element):

        try:

            message = json.loads(element.decode("utf-8"))

            yield {
                "order_id": message.get("order_id"),
                "customer_id": int(message.get("customer_id")),
                "product_id": int(message.get("product_id")),
                "category": message.get("category"),
                "price": float(message.get("price")),
                "quantity": int(message.get("quantity")),
                "payment_method": message.get("payment_method"),
                "order_status": message.get("order_status"),
                "order_timestamp": message.get("order_timestamp"),
                "customer_city": message.get("customer_city"),
                "customer_state": message.get("customer_state"),
                "customer_country": message.get("customer_country")
            }

        except Exception as e:

            logging.error(f"Error processing message: {e}")

# -----------------------------------------
# PIPELINE
# -----------------------------------------

def run():

    # pipeline_options = PipelineOptions(
    #     runner="DirectRunner",
    #     streaming=True,
    #     save_main_session=True
    # )

    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project="top-vial-497707-r1",
        region="asia-south1",
        temp_location="gs://top-vial-497707-r1-dataflow-temp/temp",
        staging_location="gs://top-vial-497707-r1-dataflow-temp/staging",
        job_name="retail-streaming-job",
        streaming=True,
        save_main_session=True,
        machine_type="e2-standard-2",
        num_workers=1,
        max_num_workers=2,
        experiments=["use_runner_v2"]
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:

        (
            pipeline

            | "Read from PubSub" >> beam.io.ReadFromPubSub(
                subscription=SUBSCRIPTION
            )

            | "Parse JSON Messages" >> beam.ParDo(
                ParseMessage()
            )

            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                TABLE_SPEC,

                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,

                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

# -----------------------------------------
# MAIN
# -----------------------------------------

if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)

    run()
