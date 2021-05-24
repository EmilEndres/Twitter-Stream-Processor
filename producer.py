import getopt
import sys
import time
from json import dumps

import tweepy
from kafka import KafkaProducer


def get_api_from_key_file(key_file):
    with open(key_file, 'r') as keys:
        consumer_key = 'nt9IMcBqnGDbmonf7hlB9Xxzq', keys.readline().rstrip()
        consumer_secret = 'lA5F5H9zoUtumWmPbeFwWIHfd5sTgM9mG41HN6degquUoirD61', keys.readline().rstrip()
        access_token = '1381549125624344577-pjYUzjmhpOL9dBZq6MYpwjHTJobJfs', keys.readline().rstrip()
        access_token_secret = 'YRDr91DPXoGaRMsIQnjMEfgKlCRZi2R8ek9G30etb2oY2', keys.readline().rstrip()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return tweepy.API(auth)


class MyStreamListener(tweepy.StreamListener):
    def __init__(self, producer):
        super().__init__()
        self.producer = producer

    def on_data(self, data):
        self.producer.send("tweets", data)
        print(data)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            return False


def stream(api):
    myStreamListener = MyStreamListener(
        KafkaProducer(bootstrap_servers=['34.204.199.146:9092'], value_serializer=lambda x: dumps(x).encode('utf-8')))
    myStream = tweepy.streaming.Stream(auth=api.auth, listener=myStreamListener)
    try:
        print('Start tweet streaming.')
        myStream.sample(languages=['en'])
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        print('Done.')
        myStream.disconnect()


def trends(api: tweepy.API):
    producer = KafkaProducer(bootstrap_servers=['34.204.199.146:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    trending_topics = api.trends_place(23424975)[0]['trends']
    print('Start trending topics streaming')
    while True:
        try:
            i = 0
            for topic in trending_topics:
                i += 1
                print(str(i) + ") " + topic['name'])
            print()
            producer.send("trending_topics", trending_topics)
            time.sleep(60)
        except KeyboardInterrupt:
            print("Stopped.")
            break
    print('Done.')


if __name__ == '__main__':
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, "hk:tw", ["help", "keys=", "topics", "tweets"])
    except getopt.GetoptError:
        print('try "python producer.py -k(path2keyfile) -(t,w)"')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('try "python producer.py -k(path2keyfile) -(t,w)"')
            sys.exit()
        elif opt in ("-k", "--keys"):
            api = get_api_from_key_file(arg)
        elif opt in ("-t", "--topics"):
            trends(api)
        elif opt in ("-w", "--tweets"):
            stream(api)

