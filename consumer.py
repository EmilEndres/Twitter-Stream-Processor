import getopt
import re
import sys
from json import loads
from datetime import datetime, timedelta

from dateutil import parser
from kafka import KafkaConsumer, TopicPartition

from wordcloud import WordCloud, STOPWORDS

import matplotlib.pyplot as plt


def plot_wordcloud(text):
    stopwords = set(STOPWORDS)
    stopwords.update(['https', 'http', 't', 'RT', 'co', 's', 'u', 'will'])

    # Generate a word cloud image
    wordcloud = WordCloud(stopwords=stopwords, background_color="white").generate(text)

    # Display the generated image:
    # the matplotlib way:
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.show()


def trending_twits(minutes_ago: int, tweet_to_display: int):
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        max_poll_records=1000000,
        enable_auto_commit=True,
        group_id='my-group')
    partition = TopicPartition('tweets', 0)
    consumer.assign([partition])
    timestamp = int((datetime.now() - timedelta(minutes=minutes_ago)).timestamp() * 1000)
    offset = consumer.offsets_for_times({partition: timestamp})
    offset = offset[partition][0]
    consumer.seek(partition, offset)
    sentences = ""
    tweets = []
    now = datetime.utcnow()
    print(now)
    for message in consumer:
        message = loads(loads(message.value))
        print(message['created_at'])
        dt = parser.parse(message['created_at'])
        if 'retweeted_status' in message:
            rt = message['retweeted_status']
            score = int(2 * rt['retweet_count'] + rt['favorite_count'] + rt['reply_count'] + 2 * rt['quote_count'])
            tweet = {'text': rt['text'], 'by': rt['user']['name'], 'score': score}
            tweets.append(tweet)

        if (now.hour == dt.hour) and (now.minute == dt.minute):
            break
        print("\n----------------------------------------\n")
    tweets.sort(key=lambda x: x['score'], reverse=True)
    i = 0
    for tweet in tweets:
        i += 1
        sentences += re.sub(r'^https?:\/\/.*[\r\n]*', '', tweet['text'], flags=re.MULTILINE)
        sentences += ' '
        print(str(i) + ") " + tweet['text'] + '\n' + 'by ' + tweet['by'] + '\nscore: ' + str(tweet['score']))
        print()
        if i >= tweet_to_display:
            break
    plot_wordcloud(sentences)


def trending_topics(minutes_ago: int, trends_to_display: int):
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        max_poll_records=1000000,
        enable_auto_commit=True,
        group_id='my-group')
    partition = TopicPartition('trending_topics', 0)
    consumer.assign([partition])
    timestamp = int((datetime.now() - timedelta(minutes=minutes_ago)).timestamp() * 1000)
    offset = consumer.offsets_for_times({partition: timestamp})
    offset = offset[partition][0]
    consumer.seek(partition, offset)
    sentences = ""
    trends = {}
    now = datetime.utcnow()
    print(now)
    for message in consumer:
        topics = loads(message.value)
        i = 50
        for topic in topics:
            if topic['name'] in trends:
                trends[topic['name']] = trends[topic['name']] + i
            else:
                trends[topic['name']] = i
            i -= 1
        end = consumer.end_offsets([partition])[partition]
        current_position = (consumer.position(partition))
        if end <= current_position:
            break
    sorted_keys = sorted(trends, key=trends.get, reverse=True)
    j = 0
    for key in sorted_keys:
        sentences += key
        sentences += ' '
        j += 1
        print(str(j) + ') ' + key)
        if j >= trends_to_display:
            break
    plot_wordcloud(sentences)


if __name__ == '__main__':
    # Remove 1st argument from the
    # list of command line arguments
    argv = sys.argv[1:]
    function = None
    min = 0
    list = 1
    try:
        opts, args = getopt.getopt(argv, "htwm:l:",
                                   ['topics', 'tweets',
                                    "minutes=", "list="])
    except getopt.GetoptError:
        print('try "python consumer.py -(t,w) -m(int) -l(int)"\n '
              '-t for trending topics or -w for trending tweets\n'
              '-m for minutes to consider\n'
              '-l list top l topics or tweets\n')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('try "python consumer.py -(t,w) -m(int) -l(int)"\n '
                  '-t for trending topics or -w for trending tweets\n'
                  '-m for minutes to consider\n'
                  '-l list top l topics or tweets\n')
            sys.exit()
        elif opt in ("-t", "--topics"):
            function = trending_topics
        elif opt in ("-w", "--tweets"):
            function = trending_twits
        elif opt in ("-m", "--minutes"):
            min = int(arg)
        elif opt in ("-l", "--list"):
            list = int(arg)
    function(min, list)
