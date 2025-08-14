import base64
import boto3
import time
import logging
import os
from dotenv import load_dotenv
from src.transfom.transformer import Transformer
import io


load_dotenv()


class LambdaConsume:
    def __init__(self):
        self.S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
        self.PROJECT_NAME = os.getenv("PROJECT_NAME")
        self.AWS_REGION = os.getenv('AWS_REGION')
        self.STREAM_NAME = os.getenv("STREAM_NAME")
        self.SHARD_ID = os.getenv("SHARD_ID")
        self.SHARD_ITERATOR_TYPE = os.getenv("SHARD_ITERATOR_TYPE")
        self.LIMIT_RECORD = int(os.getenv("LIMIT_RECORD", "100"))
        self.transformer = Transformer()
        self.s3 = boto3.client('s3', region_name=self.AWS_REGION)
        self.bucket = self.S3_BUCKET_NAME
        self.logger = logging.getLogger(self.__class__.__name__)

    def save_to_s3(self, df):
        timestamp = int(time.time())
        key = f"{self.PROJECT_NAME}/{timestamp}.parquet"
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        return key

    def handle_event(self, event):
        records = event.get('Records', [])
        if not records:
            self.logger.error(f"❌ No records found.")
            return

        df = self.transformer.transform_data(records)
        s3_key = self.save_to_s3(df)
        self.logger.info(f"✅Saved processed data to S3: {s3_key}")

    def stream_kinesis_records(self):
        kinesis = boto3.client('kinesis', region_name=self.AWS_REGION)

        # Get latest active shard
        shard_response = kinesis.describe_stream(StreamName=self.STREAM_NAME)
        active_shard = [
            s for s in shard_response['StreamDescription']['Shards']
            if 'EndingSequenceNumber' not in s['SequenceNumberRange']
        ][0]

        # Initial shard iterator
        response = kinesis.get_shard_iterator(
            StreamName=self.STREAM_NAME,
            ShardId=active_shard['ShardId'],
            ShardIteratorType=self.SHARD_ITERATOR_TYPE
        )
        shard_iterator = response['ShardIterator']

        self.logger.info("🚀 Starting Kinesis stream...")

        while True:
            records_response = kinesis.get_records(
                ShardIterator=shard_iterator,
                Limit=self.LIMIT_RECORD
            )
            records = records_response['Records']
            # 🔁 update iterator
            shard_iterator = records_response['NextShardIterator']

            if records:
                event = {
                    "Records": [
                        {
                            "kinesis": {
                                "data": base64.b64encode(r['Data']).decode('utf-8')
                            }
                        }
                        for r in records
                    ]
                }
                self.handle_event(event)
            else:
                self.logger.info("⏳ No new records. Waiting...")

            time.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    consumer = LambdaConsume()

    try:
        consumer.stream_kinesis_records()
    except KeyboardInterrupt:
        consumer.logger.info("🛑 Stopped by user.")
