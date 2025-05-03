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


tickers = ['TSLA', 'NIO', 'BYDDF', 'NEE', 'ENPH', 'PLUG']

data = yf.download(tickers, start="2023-02-15", end="2025-02-15", group_by='ticker')

closing_prices = pd.DataFrame({ticker: data[ticker]['Close'] for ticker in tickers})

print(closing_prices.describe())

closing_prices.to_csv("stock_prices.csv")

closing_prices.plot(figsize=(14, 7), title="Stock Closing Prices of Energy and EV Companies")
plt.xlabel("Date")
plt.ylabel("Price (USD)")
plt.legend(tickers)
plt.show()

daily_returns = closing_prices.pct_change()

daily_returns.plot(figsize=(14, 7), title="Daily Returns of Energy and EV Stocks")
plt.xlabel("Date")
plt.ylabel("Daily Return")
plt.show()