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


topic = "Electric vehicle"
wiki_wiki = wikipediaapi.Wikipedia(
    language='en',
    user_agent='EV_Scrapping/1.0 (https://en.wikipedia.org/wiki/Electric_vehicle#; hashimshabbir752@gmail.com)'
    )

page = wiki_wiki.page(topic)

if page.exists():
    with open(f"wikipedia_{topic}_data.txt", "w", encoding="utf-8") as file:
        file.write(page.text)
    print(f"Wikipedia data for {topic} saved to text file.")

else:
    print(f"Wikipedia page for {topic} does not exist.")
    
    
wikipedia_text = page.text
stop_words = set(stopwords.words('english'))

words = [word for word in text.split() if word.isalpha() and word.lower() not in stop_words]

word_counts = Counter(words)
most_common_words = word_counts.most_common(top_n)

words, counts = zip(*most_common_words)
plt.figure(figsize=(10, 6))
plt.bar(words, counts)
plt.title(f"Top {top_n} Most Frequent Words in Wikipedia Article")
plt.xlabel("Words")
plt.ylabel("Frequency")
plt.xticks(rotation=45)
plt.show()
