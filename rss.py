import asyncio
import aiohttp
import aiomysql
import logging
import datetime
import os
from feedparser import parse
from telegram import Bot

# RSS 源列表
RSS_FEEDS = [
    'https://blog.090227.xyz/atom.xml',  # CM
    'https://www.freedidi.com/feed', # 零度解说
   # 'https://rsshub.app/bilibili/hot-search', # bilibili
   # 'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5c91d2e23882afa09dff4901', # 36氪 - 24小时热榜
   # 'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5cac99a7f5648c90ed310e18', # 微博热搜
   # 'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5cf92d7f0cc93bc69d082608', # 百度热搜榜
   # 'https://rsshub.app/guancha/headline', # 观察网
   # 'https://rsshub.app/zaobao/znews/china', # 联合早报
   # 'https://36kr.com/feed',    # 36氪 
    # 添加更多 RSS 源
]

SECOND_RSS_FEEDS = [
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCUNciDq-y6I6lEQPeoP-R5A', # 苏恒观察
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCMtXiCoKFrc2ovAGc1eywDg', # 一休
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCOSmkVK2xsihzKXQgiXPS4w', # 历史哥
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCNiJNzSkfumLB7bYtXcIEmg', # 真的很博通
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCii04BCvYIdQvshrdNDAcww', # 悟空的日常
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCJMEiNh1HvpopPU3n9vJsMQ', # 理科男士
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCYjB6uufPeHSwuHs8wovLjg', # 中指通
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCSs4A6HYKmHA2MG_0z-F0xw', # 李永乐老师
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCZDgXi7VpKhBJxsPuZcBpgA', # 可恩Ke En
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCSYBgX9pWGiUAcBxjnj6JCQ', # 郭正亮頻道
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCbCCUH8S3yhlm7__rhxR2QQ', # 不良林
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCXkOTZJ743JgVhJWmNV8F3Q', # 寒國人
    'https://www.youtube.com/feeds/videos.xml?channel_id=UC2r2LPbOUssIa02EbOIm7NA', # 星球熱點
    'https://www.youtube.com/feeds/videos.xml?channel_id=UC000Jn3HGeQSwBuX_cLDK8Q', # 我是柳傑克
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCG_gH6S-2ZUOtEw27uIS_QA', # 7Car小七車觀點
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCJ5rBA0z4WFGtUTS83sAb_A', # POP Radio聯播網 官方頻道
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCiwt1aanVMoPYUt_CQYCPQg', # 全球大視野
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCQoagx4VHBw3HkAyzvKEEBA', # 科技共享
    'https://www.youtube.com/feeds/videos.xml?channel_id=UC7Xeh7thVIgs_qfTlwC-dag', # Marc TV
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCQO2T82PiHCYbqmCQ6QO6lw', # 月亮說
]

# 从环境变量获取配置信息
DB_HOST = os.getenv('DB_HOST', 'default_host')
DB_NAME = os.getenv('DB_NAME', 'default_db')
DB_USER = os.getenv('DB_USER', 'default_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'default_password')

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', 'default_token')
SECOND_TELEGRAM_BOT_TOKEN = os.getenv('SECOND_TELEGRAM_BOT_TOKEN', 'default_token')
ALLOWED_CHAT_IDS = os.getenv('ALLOWED_CHAT_IDS', 'default_chat_id').split(',')
SECOND_ALLOWED_CHAT_IDS = os.getenv('SECOND_ALLOWED_CHAT_IDS', 'default_chat_id').split(',')

async def fetch_feed(session, feed):
    try:
        async with session.get(feed, timeout=88) as response:
            response.raise_for_status()
            content = await response.read()
            return parse(content)
    except Exception as e:
        logging.error(f"Error fetching {feed}: {e}")
        return None

async def send_message(bot, chat_id, text):
    try:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode='Markdown')
        logging.info(f"Message sent to {chat_id}: {text}")
    except Exception as e:
        logging.error(f"Markdown send failed for {chat_id}: {e}")
        await bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')  # 使用 HTML 作为回退格式
        logging.info(f"Fallback text message sent to {chat_id}: {text}")

async def process_feed(session, feed, sent_entries, pool, bot, allowed_chat_ids, table_name):
    feed_data = await fetch_feed(session, feed)
    if feed_data is None:
        return []

    new_entries = []
    for entry in feed_data.entries:
        subject = entry.title if entry.title else None
        url = entry.link if entry.link else None
        message_id = f"{subject}_{url}" if subject and url else None

        if (url, subject, message_id) not in sent_entries:
            message = f"*{entry.title}*\n{entry.link}"
            for chat_id in allowed_chat_ids:
                await send_message(bot, chat_id, message)
            new_entries.append((url, subject, message_id))
            
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            await save_sent_entry_to_db(pool, url if url else current_time, subject if subject else current_time, message_id if message_id else current_time, table_name)
            sent_entries.add((url if url else current_time, subject if subject else current_time, message_id if message_id else current_time))
            await asyncio.sleep(6)
            
    return new_entries

async def connect_to_db_pool():
    try:
        pool = await aiomysql.create_pool(
            host=DB_HOST,
            db=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            minsize=1,
            maxsize=10
        )
        return pool
    except Exception as e:
        logging.error(f"Error while creating MySQL connection pool: {e}")
        return None

async def load_sent_entries_from_db(pool, table_name):
    try:
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(f"SELECT url, subject, message_id FROM {table_name}")
                rows = await cursor.fetchall()
                return {(row[0], row[1], row[2]) for row in rows}
    except Exception as e:
        logging.error(f"Error fetching sent entries: {e}")
        return set()

async def save_sent_entry_to_db(pool, url, subject, message_id, table_name):
    try:
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"INSERT IGNORE INTO {table_name} (url, subject, message_id) VALUES (%s, %s, %s)", 
                    (url, subject, message_id)
                )
                await connection.commit()
                logging.info(f"Saved sent entry: {url}, {subject}, {message_id} to {table_name}")
    except Exception as e:
        logging.error(f"Error saving sent entry: {e}")

async def main():
    pool = await connect_to_db_pool()
    if pool is None:
        logging.error("Failed to create the database connection pool. Exiting.")
        return

    sent_entries = await load_sent_entries_from_db(pool, "sent_rss")
    sent_entries_second = await load_sent_entries_from_db(pool, "sent_youtube")

    async with aiohttp.ClientSession() as session:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        tasks = [process_feed(session, feed, sent_entries, pool, bot, ALLOWED_CHAT_IDS, "sent_rss") for feed in RSS_FEEDS]
        results = await asyncio.gather(*tasks)

        second_bot = Bot(token=SECOND_TELEGRAM_BOT_TOKEN)
        tasks_second = [process_feed(session, feed, sent_entries_second, pool, second_bot, SECOND_ALLOWED_CHAT_IDS, "sent_youtube") for feed in SECOND_RSS_FEEDS]
        results_second = await asyncio.gather(*tasks_second)

    pool.close()
    await pool.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
