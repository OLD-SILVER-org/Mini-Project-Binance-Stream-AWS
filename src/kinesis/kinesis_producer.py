import boto3
import json
import os
import logging
import websockets
import asyncio
from src.kinesis.topic_creator import TopicCreator

from dotenv import load_dotenv

load_dotenv()


class KinesisProducer:
    def __init__(self):
        self.STREAM_NAME = os.getenv('STREAM_NAME')
        self.AWS_REGION = os.getenv('AWS_REGION')
        self.PARTITION_KEY = os.getenv("PARTITION_KEY")
        self.WSS_ENDPOINT = os.getenv("WSS_ENDPOINT")
        self.STREAM_TYPE = os.getenv("STREAM_TYPE")
        self.topic_creator = TopicCreator()
        self.TOPCOIN = self.topic_creator.get_TOPCOIN()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = boto3.client('kinesis', region_name=self.AWS_REGION)
        if not TopicCreator.TOPCOIN:
            "TOPCOIN list is empty. ProducerManager might not function as expected."

        self.logger.info(
            f"📌 TOPCOIN from TopicCreator: {TopicCreator.TOPCOIN}")

    def send_event(self, event_data):
        response = self.client.put_record(
            StreamName=self.STREAM_NAME,
            Data=json.dumps(event_data),
            PartitionKey=self.PARTITION_KEY
        )
        return response

    async def fetch_stream(self, symbol):
        url = f"{self.WSS_ENDPOINT}/{symbol}@{self.STREAM_TYPE}"
        while True:
            try:
                async with websockets.connect(url) as ws:
                    self.logger.info(f"📡 Connected to {url}")
                    while True:
                        message = await ws.recv()
                        # Process the message
                        self.send_event(message)
                        # limit One message per second
                        await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(
                    f"🔄 WebSocket error for {url}: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)

    async def start_publish(self):
        """Start multiple WebSocket connections concurrently"""
        tasks = []
        for symbol in self.TOPCOIN:
            self.logger.info(
                f"📡 Preparing to start WebSocket stream for {symbol}@{self.STREAM_TYPE}"
            )
            tasks.append(self.fetch_stream(symbol))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.info(f"TOPCOIN after initialization: {TopicCreator.TOPCOIN}")
    kinesis = KinesisProducer()
    asyncio.run(kinesis.start_publish())
    # topic_creator.create_topic() # This is already called in __init__ if TOPCOIN is populated
