import praw
from pytrends.request import TrendReq
import pandas as pd
import csv
import requests
import wikipediaapi
import matplotlib.pyplot as plt
from collections import Counter
import nltk
from nltk.corpus import stopwords
import datetime
import yfinance as yf
from kaggle.api.kaggle_api_extended import KaggleApi
import kaggle
import seaborn as sns


subreddit_name= 'ElectricVehicles'
keywords = ["electric vehicle", "EV"]
limit=200

reddit = praw.Reddit(
    client_id="TzjMkg0bTtTGMml0f8dHnQ",
    client_secret="K4lFzMci8yfeNjdYOX7FfceUzvb4pw",
    user_agent="EV_Scrapping/1.0 by Personal_Salad_6265"
)


subreddit = reddit.subreddit(subreddit_name)
posts = []

for post in subreddit.hot(limit=limit):
    if any(keyword.lower() in post.title.lower() for keyword in keywords):
        posts.append({
            "title": post.title,
            "post_text": post.selftext,
            "author": str(post.author),
            "date": post.created_utc,
            "upvotes": post.score*post.upvote_ratio,
            "subreddit": subreddit_name
        })

with open(f"reddit_{subreddit_name}_data.csv", "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = ["title", "post_text", "author", "date", "upvotes", "subreddit"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(posts)

print(f"Reddit data for {subreddit_name} saved to CSV.")

nltk.download('stopwords')
stop_words = set(stopwords.words('english'))

reddit_data = pd.read_csv("reddit_ElectricVehicles_data.csv")

text = " ".join(reddit_data['title'].dropna().tolist()).lower()
top_n = 20
words = [word for word in text.split() if word.isalpha() and word not in stop_words]

word_counts = Counter(words)
most_common_words = word_counts.most_common(top_n)

words, counts = zip(*most_common_words)
plt.figure(figsize=(10, 6))
plt.bar(words, counts, color='skyblue')
plt.title(f"Top {top_n} Most Frequent Words in Reddit {'title'.capitalize()}")
plt.xlabel("Words")
plt.ylabel("Frequency")
plt.xticks(rotation=45)
plt.show()

reddit_data['date'] = pd.to_datetime(reddit_data['date'], unit='s')

avg_upvotes = reddit_data.groupby(reddit_data['date'].dt.date)['upvotes'].mean()

plt.figure(figsize=(10, 6))
plt.plot(avg_upvotes.index, avg_upvotes.values)
plt.title("Average Upvotes Over Time")
plt.xlabel("Date")
plt.ylabel("Average Upvotes")
plt.grid(True)
plt.show()