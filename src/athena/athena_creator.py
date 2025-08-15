from src.athena.athena_ticker import AthenaTicker
from dotenv import load_dotenv
import os
import boto3
import time
import logging
load_dotenv()


class AthenaCreator:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(AthenaCreator, cls).__new__(
                cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        self.AWS_REGION = os.getenv("AWS_REGION")
        self.S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
        self.ATHENA_MINI_DB = os.getenv("ATHENA_MINI_DB")
        self.S3_STAGING_DIR = os.getenv("S3_STAGING_DIR")
        self.STREAM_TYPES = os.getenv("STREAM_TYPES")
        self.PROJECT_NAME = os.getenv("PROJECT_NAME")

        self.athena_client = boto3.client(
            "athena",
            region_name=self.AWS_REGION
        )

        self.athena_ticker = AthenaTicker(
            self.athena_client, self.S3_STAGING_DIR, self.S3_BUCKET_NAME, self.ATHENA_MINI_DB, self.PROJECT_NAME
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_client(self):
        return self.athena_client

    def run_query(self, query: str, database: str = None) -> bool:  # type: ignore
        params = {
            "QueryString": query,
            "ResultConfiguration": {
                "OutputLocation": f"{self.S3_STAGING_DIR}/"
            },
        }
        if database:
            params["QueryExecutionContext"] = {"Database": database}

        response = self.get_client().start_query_execution(**params)
        query_execution_id = response["QueryExecutionId"]

        while True:
            result = self.get_client().get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = result["QueryExecution"]["Status"]["State"]
            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(1)

        self.logger.info(f"📌Query status : {status}")
        return status == "SUCCEEDED"

    def create_database(self):
        query = f"CREATE DATABASE IF NOT EXISTS {self.ATHENA_MINI_DB}"
        if self.run_query(query):
            self.logger.info("✅ Database created successfully")
        else:
            self.logger.error("❌ Failed to create database")

    def run_athena(self):
        self.create_database()
        self.athena_ticker.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    athena = AthenaCreator()
    try:
        athena.run_athena()
    except KeyboardInterrupt:
        athena.logger.info("🛑 Stopped by user.")
