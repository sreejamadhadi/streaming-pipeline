#!/usr/bin/sh
# -*- coding: utf-8 -*-
import tweepy
import json
import logging
from concurrent import futures
from google.cloud import pubsub_v1
from google.auth import jwt


class PublishToTopic(tweepy.StreamListener):
    def __init__(self):
        super().__init__()
        self.project_id = 'crafty-nomad-320217'
        self.topic_id = 'streaming-pipeline'
        self.twitter_api = None
        self.publisher = None
        self.publish_futures = []

    def twitter_api_connect(self, twitter_credentials):
        auth = tweepy.OAuthHandler(twitter_credentials['API_KEY'], twitter_credentials['API_SECRET'])
        auth.set_access_token(twitter_credentials['ACCESS_TOKEN'], twitter_credentials['ACCESS_TOKEN_SECRET'])
        api = tweepy.API(auth)
        self.twitter_api = api

    def pubsub_connect(self, json_key_file):
        service_account_info = json.load(open(json_key_file))
        audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
        credentials = jwt.Credentials.from_service_account_info(service_account_info, audience=audience)
        publisher = pubsub_v1.PublisherClient(credentials=credentials)
        self.publisher = publisher

    def get_callback(self, publish_future, data):
        def callback(publish_future):
            try:
                logging.info(publish_future.result(timeout=60))
            except:
                logging.warning(f'Publishing {data} timed out.')

        return callback

    def publish_to_topic(self, message):
        logging.info("Entered Publish to topic")
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        publish_future = self.publisher.publish(topic_path, message.encode('utf8'))
        publish_future.add_done_callback(self.get_callback(publish_future, message))
        self.publish_futures.append(publish_future)
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)

    def on_data(self, raw_data):
        logging.info(raw_data)
        self.publish_to_topic(raw_data)

    def on_error(self, status_code):
        if status_code == 420:
            return False


with open('credentials.json') as file:
    twitter_credentials = json.load(file)
gcp_json_key = "streamingPipeline.privatekey.json"
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    publish_to_topic = PublishToTopic()
    publish_to_topic.twitter_api_connect(twitter_credentials=twitter_credentials)
    publish_to_topic.pubsub_connect(json_key_file=gcp_json_key)
    stream = tweepy.Stream(auth=publish_to_topic.twitter_api.auth, listener=publish_to_topic)
    stream.filter(track=["stocks", "stock market","stock value","stock price"], languages=['en'])
