import base64
import json
from pandas import DataFrame
from json import loads
import logging
from google.cloud import storage

class LoadToStorage:
    def __init__(self):
        self.bucket_name = 'streaming_data_bucket'

    def parse_data(self, data):
        try:
            return [data['id'], data['created_at'], data['text'].replace(',' , ''),
                             data['source'], data['reply_count'], data['retweet_count'], data['favorite_count'],
                             data['user']['id'], data['user']['name'].replace(',' , ''),
                             data['user']['location'].replace(',' , '') if data['user']['location'] else None,
                             data['user']['followers_count'], data['user']['friends_count'],
                             data['user']['listed_count'], data['user']['favourites_count'],
                             data['user']['statuses_count'], data['user']['created_at']]
        except Exception as e:
            logging.error(f"Parse Error: {str(e)}")
            raise

    def transform_data(self, data):
        try:
            df = DataFrame(data).T
            if not df.empty:
                logging.info(f"Created DataFrame with {df.shape[0]} rows and {df.shape[1]} columns")
            else:
                logging.warning(f"Created empty DataFrame")
            return df
        except Exception as e:
            logging.error(f"Error during creating DataFrame - {str(e)}")
            raise

    def write_to_gcs(self, df, filename):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(f'{filename}.csv')
        blob.upload_from_string(data=df.to_csv(index=False, header=None), content_type='text/csv')
        logging.info('Sucessfully written file to Cloud storage.')

    def validate_and_decode_to_json(self, event):
        """Extracting, Validating and decoding the published data."""

        logging.info(
            f"TODO logs by understanding the context"
        )

        if "data" in event:
            message = base64.b64decode(event['data']).decode('utf-8')
            logging.info(message);
            return json.loads(message)
        else:
            logging.error("Improper Message Format")
            return ""

def pubsub_listener(event, context):
    """ Pubsub subscriber: An event is triggered for every message published in to the pubsub/topic.
    Args:
         event: Event payload.
         context: Metadata for the event.
    """
    pubsub_to_storage = LoadToStorage()
    parsed_data = pubsub_to_storage.parse_data(pubsub_to_storage.validate_and_decode_to_json(event))
    pubsub_to_storage.write_to_gcs(pubsub_to_storage.transform_data(parsed_data), 'stocksData-' + str(parsed_data[0]))