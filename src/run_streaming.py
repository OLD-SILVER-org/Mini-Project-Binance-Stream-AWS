from src.athena.athena_creator import AthenaCreator
from src.kinesis.topic_creator import TopicCreator
from src.kinesis.kinesis_producer import KinesisProducer
from src.transfom.lambda_consume import LambdaConsume
from concurrent.futures import ThreadPoolExecutor, wait
import asyncio
import logging


# Main class to orchestrate the entire streaming process.
class RunStreaming:
    def __init__(self):
        logging.basicConfig(
            # Configure basic logging for the application.
            level=logging.INFO,
            format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.topic_creator = TopicCreator()
        self.producer = KinesisProducer()
        self.consumer = LambdaConsume()
        # Initialize various components needed for the streaming pipeline.
        self.athena = AthenaCreator()

    @staticmethod
    def run_async_producer(producer):
        # Static method to run the Kafka producer's asynchronous publishing process.
        asyncio.run(producer.start_publish())

    def run(self):
        # Main method to start all parts of the streaming application
        # Create Athena DB and tables after Spark might have written some data.
        self.athena.run_athena()
        # Run producer + Spark pipelines concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit Kafka producer and Spark streaming pipelines to run in parallel.
            producer_future = executor.submit(
                self.run_async_producer, self.producer)

            ticker_pipeline_future = executor.submit(
                self.consumer.stream_kinesis_records())

            wait([producer_future, ticker_pipeline_future])


if __name__ == "__main__":
    logging.basicConfig(
        # Configure basic logging for the application.
        level=logging.INFO,
        format="[%(asctime)s] %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        logging.info("📡🟢📡Starting Spark streaming pipeline📡🟢📡...")
        run = RunStreaming()
        run.run()
    except Exception as e:
        logging.error(f"❌ Error when running Spark pipeline{e}")
    finally:
        logging.info("✅✅Spark streaming pipeline finished running✅✅")
