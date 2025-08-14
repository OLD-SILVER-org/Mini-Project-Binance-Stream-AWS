
import os
import requests
import json
import logging
from dotenv import load_dotenv

load_dotenv()


class TopicCreator:
    # Class attribute
    TOPCOIN = []

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.URL_TOP = os.getenv("URL_TOP")
        self.LIMIT = int(os.getenv("LIMIT", 10))
        # init value for TOPCOIN
        self.get_top_coins()

    @classmethod
    def get_TOPCOIN(cls):
        """Class method to access TOPCOIN."""
        return cls.TOPCOIN

    def get_top_coins(self):
        try:
            response = requests.get(self.URL_TOP)  # type: ignore
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            data = response.json()
            # Filter the coins that end with "USDT"
            filtered_coins = [
                coin
                for coin in data
                if isinstance(coin, dict)
                and "symbol" in coin
                and coin["symbol"].endswith("USDT")
            ]
            # Sort the filtered coins by quoteVolume and take the top
            top_coins = sorted(
                filtered_coins,
                key=lambda x: float(x.get("quoteVolume", 0)),
                reverse=True,
            )[: self.LIMIT]
            TopicCreator.TOPCOIN = [coin["symbol"].lower()
                                    for coin in top_coins]
            self.logger.info(
                f"Successfully fetched and updated TOPCOIN list: {TopicCreator.TOPCOIN}"
            )
        except requests.exceptions.RequestException as e:
            self.logger.error(
                f"Failed to get top coins from {self.URL_TOP}: {e}")
            TopicCreator.TOPCOIN = []  # Ensure TOPCOIN is empty on failure
        except json.JSONDecodeError as e:
            self.logger.error(
                f"Failed to decode JSON response from {self.URL_TOP}: {e}"
            )
            TopicCreator.TOPCOIN = []


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    topic_creator = TopicCreator()
    logging.info(f"TOPCOIN after initialization: {TopicCreator.TOPCOIN}")
    # topic_creator.create_topic() # This is already called in __init__ if TOPCOIN is populated
