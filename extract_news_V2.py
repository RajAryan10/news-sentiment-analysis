from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
from requests_html import HTMLSession
import logging

logger = logging.getLogger(__name__)


def date_validator(time_stamp, past_months):
    if time_stamp is None:
        return True

    time_format = "%d %b, %Y %I:%M:%S %p"
    article_time_stamp = datetime.strptime(time_stamp, time_format)
    # article_date = datetime.strftime(article_time_stamp, '%Y-%m-%d')
    time_duration = datetime.now() - timedelta(days=past_months * 30)

    if article_time_stamp >= time_duration:
        return True
    return False

def get_news_content(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    response = requests.get(url, headers=headers)
    return response



def extract_news(news_url, past_months):
    response = get_news_content(news_url)
    soup = BeautifulSoup(response.content, "lxml")
    news_items = []
    logger.info("Retrieving news items...")
    for idx, article in enumerate(soup.find_all("div", class_="cartHolder")):
        try:
            date = article.get("data-vars-story-time", None)
            title = article.get("data-vars-story-title", "Title Not Available")
            link = article.get("data-weburl", "Link Not Nvailable")
            descriptions = article.select("h2.sortDec")
            if descriptions:
                description = str(descriptions[0]).strip().replace('<h2 class="sortDec">',"").replace("</h2>","")
            else:
                description = "Description Not Available."

            if date_validator(date, past_months):
                news_items.append({
                    "Date" : date,
                    "Title" : title,
                    "Short Description" : description,
                    "Link" : link
                })

        except Exception as err:
            error_msg = f"Error Occured : {err}"
            logger.info(f"{error_msg}")
            continue
        # break
    return news_items


def extract_descriptions(news_items):
    logger.info("Retrieving News.....")
    for news in news_items:
        response = get_news_content(news["Link"])
        soup = BeautifulSoup(response.content, "lxml")
        for idx, desc in enumerate(soup.find_all("div", class_ = "storyDetails taboola-readmore")):
            paragraphs = desc.find_all('p')
            news_content = "\n".join(p.get_text(strip=True) for p in paragraphs)
            news["News"] = news_content
    
    return news_items


def news_crawler(news_url, past_months):
    news_items = extract_news(news_url=news_url, past_months=past_months)
    news_items_with_full_news = extract_descriptions(news_items)
    return news_items_with_full_news


if __name__ == '__main__':
    news_url = "https://www.hindustantimes.com/latest-news"
    past_months = 12
    # news_items = extract_news(news_url, past_months)
    # news_items = extract_news_v2(news_url, past_months)
    # news_items_with_full_news = extract_descriptions(news_items)
    # print(f"Length - 1 : {len(news_items)}")
    # print(f"Length - 2 : {len(news_items_with_full_news)}")
    
    news_items_with_full_news = news_crawler(news_url, past_months)

    for count, news in enumerate(news_items_with_full_news):
        print(f"{'-'*50} News Item : {count+1} {'-'*50}")
        print(news)
        
