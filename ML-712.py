# Databricks notebook source
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Cursor

consumer_key = "eVKyDWkljiavrNvQqBFOVXtn5"
consumer_secret = "Lz4jiwB3FM4xrUF2CciAFVVWd5fLXb9hNhu9AOYMTVvxugQMgS" #twitter app’s API secret Key
access_token = "895129932178829312-MFdi0eYyhcWcRkhpQyyIFNMeMQHH23m" #twitter app’s Access token
access_token_secret = "2VImPRL9w3wiwdX9QzkhyVyjB8dKFUPZwF1ZAsNRNrFRJ" #twitter app’s access token secret

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
auth_api = API(auth)

TanjaKleut_tweets = auth_api.user_timeline(screen_name = 'TanjaKleut', count = 600, include_rts = False, tweet_mode = 'extended')

final_tweets = [each_tweet.full_text for each_tweet in TanjaKleut_tweets]

print (final_tweets)

with open('/dbfs/FileStore/tables/TanjaKleut_tweets.txt', 'w') as f:
  for item in final_tweets:
    f.write("%s\n" % item)

read_tweets = []
with open('/dbfs/FileStore/tables/TanjaKleut_tweets.txt','r') as f:
  read_tweets.append(f.read())


