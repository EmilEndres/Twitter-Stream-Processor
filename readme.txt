install packages: sudo apt-get install python3-tk
                  pip install -r requirements.txt
producer usage: python producer.py -k(path2keyfile) -(t,w)
    -t trending topics or -w trending tweets
consumer usage: python consumer.py -(t,w) -m(int) -l(int)
    -t for trending topics or -w for trending tweets
    -m for minutes to consider
    -l list top l topics or tweets
