import pandas as pd
import logging
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
import tensorflow as tf
import warnings

from extract_news_V2 import news_crawler
from summarize_news import BartSummarizer
from sentiment_analysis import SentimentAnalyzer

from push_to_db_v2 import PostgresDB

warnings.filterwarnings("ignore")
tf.get_logger().setLevel('ERROR')

logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)

def prepare_prompt(news_text, news_title):
    prompt = f"""Summarize the following news article comprehensively in no more than 150 words, \
        ensuring that the summary accurately reflects the context and key points related to the given title. \
        The summary should be concise, coherent, and cover the main events, people involved, and any significant outcomes or implications.
        Article Title : {news_title}
        Article Text : {news_text}
        """
    return prompt



def process_news_items(news_items):
    news_df = pd.DataFrame(news_items)
    summarizer = BartSummarizer(max_length=200)
    sentiment_analyzer = SentimentAnalyzer()
    logger.info("Generating Summary and analysing sentiments....")
    for i, rows in news_df.iterrows():
        if pd.isna(rows["Title"]) or pd.isna(rows["News"]):
            #logger.info(f"skipping row {i+1} because of null value")
            continue    
        prompt = prepare_prompt(news_text=rows["News"], news_title=rows["Title"])
        summary = summarizer.summarize(prompt=prompt)
        sentiment = sentiment_analyzer.analyze_sentiment(summary)
        news_df.at[i, "News Summary"] = summary
        news_df.at[i, "Sentiment"] = sentiment['label']
        news_df.at[i, "Sentiment Confidence"] = sentiment['score']

    return news_df

def push_to_db(news_df):
    db = PostgresDB(dbname="mydb", user_name="rahul", password="password", host="localhost", table_name="news_analysis")
    logger.info("Writing to the database...")
    for i, rows in news_df.iterrows():
        db.insert_news(rows)
    logger.info("Pushed to Database Successfully..")



def main():
    news_website_url = "https://www.hindustantimes.com/latest-news"
    PAST_MONTHS = 12

    news_items = news_crawler(news_url=news_website_url, past_months=PAST_MONTHS)
    news_df = process_news_items(news_items)
    push_to_db(news_df)

if __name__ == "__main__":
    main()
