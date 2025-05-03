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


!kaggle datasets download willianoliveiragibin/electric-vehicle-population
!unzip electric-vehicle-population.zip

df = pd.read_csv('/content/Electric_Vehicle_Population_Data.csv')

display(df.head())

plt.figure(figsize=(12,6))
sns.countplot(data=df,x='Make')
plt.xticks(rotation=90)
plt.xlabel('Make')
plt.ylabel('Count')
plt.show()

plt.figure(figsize=(8, 6))
df['Electric Vehicle Type'].value_counts().plot.pie(autopct='%1.1f%%')
plt.title('Distribution of Electric Vehicle Types')
plt.ylabel('')
plt.show()