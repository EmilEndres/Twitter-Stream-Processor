install packages: sudo apt-get install python3-tk
                  pip install -r requirements.txt
                  
create a txt file named keys.txt storing the personal Twitter API keys without the names or quotes, in the order shown in the producer and attach it to the folder in PyCharm
                  
producer usage: python producer.py -k(path2keyfile) -(t,w)
    -t trending topics or -w trending tweets
    
consumer usage: python consumer.py -(t,w) -m(int) -l(int)
    -t for trending topics or -w for trending tweets
    -m for minutes to consider
    -l list top l topics or tweets
