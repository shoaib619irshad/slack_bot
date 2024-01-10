import json, os
from bs4  import BeautifulSoup

import mysql.connector , feedparser
from slack_sdk import WebClient
from dotenv import load_dotenv

load_dotenv('.env')

USER = os.getenv("USER_NAME")
PASSWORD = os.getenv("PASSWORD")
HOST_NAME = os.getenv("HOST_NAME")
DB= os.getenv("DBNAME")

cnx = mysql.connector.connect(user=USER, password=PASSWORD, host=HOST_NAME, database=DB)
cursor = cnx.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS feed_details (title VARCHAR(255), link VARCHAR(255), PRIMARY KEY(title))")

slack_token = os.getenv("SLACK_BOT_TOKEN")
sc = WebClient(token=slack_token)

class Feed():
    def __init__(self, url, channel):
        self.Url = url
        self.Channel = channel

feeds = [
        Feed("https://www.upwork.com/ab/feed/jobs/rss?q=node&sort=recency&job_type=hourly%2Cfixed&contractor_tier=1%2C2%2C3&proposals=0-4%2C5-9%2C10-14%2C15-19%2C20-49&budget=100-499%2C500-999%2C1000-4999%2C5000-&workload=as_needed%2Cpart_time%2Cfull_time&duration_v3=week%2Cmonth%2Csemester%2Congoing&verified_payment_only=1&connect_price=0-2%2C4%2C6&paging=0%3B50&api_params=1&securityToken=47afe9a9c98905215eff0ebb88e56c6569dcdc4d488154ffd5919b79fe7dea7de636ce5f4f17916ec086e90afb58c177a39d0220953e320ac563245149f0f2a3&userUid=700193349553074176&orgUid=700193349557268481", "#upwork_feed_node"),
        Feed("https://www.upwork.com/ab/feed/jobs/rss?q=golang&sort=recency&job_type=hourly%2Cfixed&contractor_tier=1%2C2%2C3&proposals=0-4%2C5-9%2C10-14%2C15-19%2C20-49&budget=100-499%2C500-999%2C1000-4999%2C5000-&workload=as_needed%2Cpart_time%2Cfull_time&duration_v3=week%2Cmonth%2Csemester%2Congoing&verified_payment_only=1&connect_price=0-2%2C4%2C6&paging=0%3B50&api_params=1&securityToken=47afe9a9c98905215eff0ebb88e56c6569dcdc4d488154ffd5919b79fe7dea7de636ce5f4f17916ec086e90afb58c177a39d0220953e320ac563245149f0f2a3&userUid=700193349553074176&orgUid=700193349557268481", "#upwork_feed_golang"),
        Feed("https://www.upwork.com/ab/feed/jobs/rss?q=react&sort=recency&job_type=hourly%2Cfixed&contractor_tier=1%2C2%2C3&proposals=0-4%2C5-9%2C10-14%2C15-19%2C20-49&budget=100-499%2C500-999%2C1000-4999%2C5000-&workload=as_needed%2Cpart_time%2Cfull_time&duration_v3=week%2Cmonth%2Csemester%2Congoing&verified_payment_only=1&connect_price=0-2%2C4%2C6&paging=0%3B50&api_params=1&securityToken=47afe9a9c98905215eff0ebb88e56c6569dcdc4d488154ffd5919b79fe7dea7de636ce5f4f17916ec086e90afb58c177a39d0220953e320ac563245149f0f2a3&userUid=700193349553074176&orgUid=700193349557268481", "#upwork_feed_react"),
        Feed("https://www.upwork.com/ab/feed/jobs/rss?q=python&sort=recency&job_type=hourly%2Cfixed&contractor_tier=1%2C2%2C3&proposals=0-4%2C5-9%2C10-14%2C15-19%2C20-49&budget=100-499%2C500-999%2C1000-4999%2C5000-&workload=as_needed%2Cpart_time%2Cfull_time&duration_v3=week%2Cmonth%2Csemester%2Congoing&verified_payment_only=1&connect_price=0-2%2C4%2C6&paging=0%3B50&api_params=1&securityToken=47afe9a9c98905215eff0ebb88e56c6569dcdc4d488154ffd5919b79fe7dea7de636ce5f4f17916ec086e90afb58c177a39d0220953e320ac563245149f0f2a3&userUid=700193349553074176&orgUid=700193349557268481", "#upwork_feed_python"),
        Feed("https://www.upwork.com/ab/feed/jobs/rss?q=django&sort=recency&job_type=hourly%2Cfixed&contractor_tier=1%2C2%2C3&proposals=0-4%2C5-9%2C10-14%2C15-19%2C20-49&budget=100-499%2C500-999%2C1000-4999%2C5000-&workload=as_needed%2Cpart_time%2Cfull_time&duration_v3=week%2Cmonth%2Csemester%2Congoing&verified_payment_only=1&connect_price=0-2%2C4%2C6&paging=0%3B50&api_params=1&securityToken=47afe9a9c98905215eff0ebb88e56c6569dcdc4d488154ffd5919b79fe7dea7de636ce5f4f17916ec086e90afb58c177a39d0220953e320ac563245149f0f2a3&userUid=700193349553074176&orgUid=700193349557268481", "#upwork_feed_python"),
        Feed("https://www.upwork.com/ab/feed/jobs/rss?q=node&sort=recency&job_type=hourly%2Cfixed&contractor_tier=1%2C2%2C3&proposals=0-4%2C5-9%2C10-14%2C15-19%2C20-49&budget=100-499%2C500-999%2C1000-4999%2C5000-&workload=as_needed%2Cpart_time%2Cfull_time&duration_v3=week%2Cmonth%2Csemester%2Congoing&verified_payment_only=1&connect_price=0-2%2C4%2C6&paging=0%3B50&api_params=1&securityToken=47afe9a9c98905215eff0ebb88e56c6569dcdc4d488154ffd5919b79fe7dea7de636ce5f4f17916ec086e90afb58c177a39d0220953e320ac563245149f0f2a3&userUid=700193349553074176&orgUid=700193349557268481", "#upwork_feed_node")
    ]


def SendSlackNotification(channel, title, link, description):
    intro_msg = json.dumps([{"title":title,
                             "text": description,
                             "title_link":link,
                             "fallback":"You are unable to choose an option",
                             "callback_id":"project_intro",
                             "color":"#3AA3E3",
                             "attachment_type":"default",
                             }])
    text ="New RSS Feed Item :"
    response = sc.chat_postMessage(channel=channel, text=text, attachments=intro_msg)
    return response


def ProcessRSSFeed(feeds):
    for feed in feeds:
        fp = feedparser.parse(feed.Url)
        feed_items = fp.entries

        for item in feed_items:
            title = item.title
            cursor.execute("SELECT title FROM feed_details WHERE title = %s", (title,))
            row = cursor.fetchall()
            if row:
                continue
            link = item.link
            description = item.description
            description = BeautifulSoup(description, features="html.parser")
            description = description.get_text()
            channel = feed.Channel
            SendSlackNotification(channel=channel, title=title, link=link, description=description)
            cursor.execute("INSERT INTO feed_details (title, link) VALUES (%s, %s)",( title, link))
            
        cnx.commit()
        cursor.close()
        cnx.close()



ProcessRSSFeed(feeds)