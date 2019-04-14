#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import findspark

findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import tweepy
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import socket
import json


# In[ ]:


consumer_key = get_ipython().run_line_magic('env', 'TWITTER_API_KEY')
consumer_secret = get_ipython().run_line_magic('env', 'TWITTER_API_SECRET_KEY')
access_token = get_ipython().run_line_magic('env', 'TWITTER_ACCESS_TOKEN')
access_secret = get_ipython().run_line_magic('env', 'TWITTER_ACCESS_TOKEN_SECRET')


# In[ ]:


class TweetListener(StreamListener):
    
    def __init__(self, csocket):
        self.client_socket = csocket
        
    def on_data(self,data):
        
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("ERROR ",e)
        return True
    
    def on_error(self, status):
        print(status)
        return True


# In[ ]:


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token,access_secret)
    
    twitter_stream = Stream(auth, TweetListener(c_socket))
    twitter_stream.filter(track=['guitar'])


# In[ ]:


if __name__ == '__main__':
    s = socket.socket()
    host = '127.0.0.1'
    port = 9997
    s.bind((host,port))
    
    print(f'listening on port {port}')
    
    s.listen(5)
    c,addr = s.accept()
    
    sendData(c)


# In[ ]:





# In[ ]:


# sc = SparkContext('local[2]','NetworkWordCount')
# ssc = StreamingContext(sc,1)
# lines = ssc.socketTextStream('localhost',9999)
# words = lines.flatMap(lambda line: line.split(' '))
# pairs = words.map(lambda word: (word,1))
# word_counts = pairs.reduceByKey(lambda num1, num2: num1+num2)
# word_counts.pprint()
# ssc.start()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




