import asyncio   
import aiohttp
import aiomysql
import logging
import datetime
import os
from feedparser import parse
from telegram import Bot

# 初始化日志记录器
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# RSS 源列表合并推送 定时24小时
RSS_FEEDS = [
    'https://rsshub.app/bilibili/hot-search',
    'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5c91d2e23882afa09dff4901',
    'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5cac99a7f5648c90ed310e18',
    'https://rss.mifaw.com/articles/5c8bb11a3c41f61efd36683e/5cf92d7f0cc93bc69d082608',
    'https://rsshub.app/guancha/headline',
    'https://rsshub.app/zaobao/znews/china',
    'https://blog.090227.xyz/atom.xml',
    'https://www.freedidi.com/feed',
    'https://36kr.com/feed',
]

# 从环境变量获取 Telegram 相关信息
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
ALLOWED_CHAT_IDS = os.environ.get("ALLOWED_CHAT_IDS")
MAX_TELEGRAM_MSG_LENGTH = 4096  # Telegram 消息字数限制

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
        if len(text) > MAX_TELEGRAM_MSG_LENGTH:
            text = text[:MAX_TELEGRAM_MSG_LENGTH] + "..."
        await bot.send_message(chat_id=chat_id, text=text, parse_mode='Markdown')
        logging.info(f"Message sent to {chat_id}: {text}")
    except Exception as e:
        logging.error(f"Markdown send failed for {chat_id}: {e}")
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')
        except Exception as fallback_e:
            logging.error(f"HTML send failed: {fallback_e}")
            await bot.send_message(chat_id=chat_id, text=text)

async def process_feed(session, feed, sent_entries, connection, bot, allowed_chat_ids, table_name):
    feed_data = await fetch_feed(session, feed)
    if feed_data is None:
        return []

    new_entries = []
    messages = []

    for entry in feed_data.entries:
        subject = entry.title if entry.title else None
        url = entry.link if entry.link else None
        message_id = f"{subject}_{url}" if subject and url else None

        if (url, subject, message_id) not in sent_entries:
            message = f"*{entry.title}*\n{entry.link}"
            messages.append(message)
            new_entries.append((url, subject, message_id))

            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            await save_sent_entry_to_db(connection, url if url else current_time, subject if subject else current_time, message_id if message_id else current_time, table_name)
            sent_entries.add((url if url else current_time, subject if subject else current_time, message_id if message_id else current_time))

    if messages:
        combined_message = "\n\n".join(messages)
        for chat_id in allowed_chat_ids:
            await send_message(bot, chat_id, combined_message)
            
    return new_entries

async def connect_to_db():
    try:
        connection = await aiomysql.connect(
            host=os.environ.get("DB_HOST"),
            db=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD")
        )
        return connection
    except Exception as e:
        logging.error(f"Error while connecting to MySQL: {e}")
        return None

async def load_sent_entries_from_db(connection, table_name):
    try:
        async with connection.cursor() as cursor:
            await cursor.execute(f"SELECT url, subject, message_id FROM {table_name}")
            rows = await cursor.fetchall()
            return {(row[0], row[1], row[2]) for row in rows}
    except Exception as e:
        logging.error(f"Error fetching sent entries: {e}")
        return set()

async def save_sent_entry_to_db(connection, url, subject, message_id, table_name):
    try:
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
    connection = await connect_to_db()
    if connection is None:
        logging.error("Failed to connect to the database. Exiting.")
        return

    sent_entries = await load_sent_entries_from_db(connection, "sent_rss2")

    async with aiohttp.ClientSession() as session:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        tasks = [process_feed(session, feed, sent_entries, connection, bot, ALLOWED_CHAT_IDS, "sent_rss2") for feed in RSS_FEEDS]
        await asyncio.gather(*tasks)

    connection.close()

if __name__ == "__main__":
    asyncio.run(main())