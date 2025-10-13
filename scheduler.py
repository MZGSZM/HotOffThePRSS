# scheduler.py
# This script is a dedicated background process for fetching and posting RSS feeds.
# It's designed to be run as a standalone service.

import os
import json
import yaml
import time
import uuid
import fcntl
import feedparser
import requests
from datetime import datetime, timedelta, timezone

# --- Set a common User-Agent for all requests ---
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/116.0"
feedparser.USER_AGENT = USER_AGENT

# --- Configuration & State Files ---
CONFIG_FILE = "config.json"
SENT_ARTICLES_FILE = "sent_articles.yaml"
FEED_STATE_FILE = "feed_state.json"

# --- Data Loading and Saving Functions ---

def load_config():
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"FEEDS": []}

def load_feed_state():
    try:
        with open(FEED_STATE_FILE, 'r') as f:
            content = f.read()
            if not content: return {}
            return json.loads(content)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_feed_state(state_data):
    with open(FEED_STATE_FILE, 'w') as f:
        json.dump(state_data, f, indent=4)

def filter_and_update_sent_articles(article_ids_to_check):
    """
    Atomically checks which articles are new and updates the sent articles file.
    This is the core fix for the race condition.
    Returns a set of article IDs that are genuinely new.
    """
    new_article_ids = set()
    try:
        with open(SENT_ARTICLES_FILE, 'r+') as f:
            # Acquire an exclusive lock, preventing any other process from reading or writing
            fcntl.flock(f, fcntl.LOCK_EX)
            
            # Read the existing sent articles
            sent_articles_list = yaml.safe_load(f) or []
            sent_articles_set = set(sent_articles_list)
            
            # Determine which of the provided articles are new
            genuinely_new_ids = set(article_ids_to_check) - sent_articles_set
            
            if genuinely_new_ids:
                # Add the new IDs to the set and convert back to a list
                updated_sent_articles_set = sent_articles_set.union(genuinely_new_ids)
                
                # Prune the list to the 10,000 most recent entries
                updated_sent_articles_list = sorted(list(updated_sent_articles_set))[-10000:]
                
                # Go back to the beginning of the file, clear it, and write the new list
                f.seek(0)
                f.truncate()
                yaml.dump(updated_sent_articles_list, f)
                
                new_article_ids = genuinely_new_ids

            # Release the lock
            fcntl.flock(f, fcntl.LOCK_UN)
            
    except FileNotFoundError:
        # If the file doesn't exist, all articles are new
        with open(SENT_ARTICLES_FILE, 'w') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            updated_sent_articles_list = sorted(list(article_ids_to_check))[-10000:]
            yaml.dump(updated_sent_articles_list, f)
            fcntl.flock(f, fcntl.LOCK_UN)
        new_article_ids = set(article_ids_to_check)
    except Exception as e:
        print(f"Error in filter_and_update_sent_articles: {e}")

    return new_article_ids


# --- Core Logic ---

def send_to_webhook(webhook_url, embed):
    """Sends a rich embed to a Discord webhook."""
    headers = {"Content-Type": "application/json"}
    payload = {"embeds": [embed]}
    try:
        response = requests.post(webhook_url, headers=headers, json=payload, timeout=10)
        if response.status_code in [200, 204]:
            return "Success"
        elif response.status_code == 429:
            print(f"Rate limited by Discord for webhook {webhook_url}.")
            return "Rate Limited"
        else:
            print(f"Error sending to webhook {webhook_url}: {response.status_code} - {response.text}")
            return f"Error: {response.status_code}"
    except requests.RequestException as e:
        print(f"Failed to connect to webhook {webhook_url}: {e}")
        return "Failed to Connect"

def check_single_feed(feed_config):
    """Checks a single feed for new articles and posts them."""
    feed_url = feed_config.get("url")
    feed_id = feed_config.get("id")
    
    headers = {"User-Agent": USER_AGENT}
    feed_data = feedparser.parse(feed_url, request_headers=headers)
    
    status_code = feed_data.get('status', 500)
    last_post_status = None
    
    if not feed_data.entries:
        if status_code != 200:
            print(f"Could not fetch feed: {feed_url} (Status: {status_code})")
        return status_code, last_post_status

    now = datetime.now(timezone.utc)
    twenty_four_hours_ago = now - timedelta(hours=24)
    
    recent_articles = []
    for entry in feed_data.entries:
        published_time = entry.get('published_parsed')
        if published_time:
            published_dt = datetime.fromtimestamp(time.mktime(published_time)).replace(tzinfo=timezone.utc)
            if published_dt >= twenty_four_hours_ago:
                recent_articles.append(entry)

    if not recent_articles:
        return status_code, last_post_status

    recent_articles.sort(key=lambda x: x.get('published_parsed', (0,)*9), reverse=True)
    
    article_id_map = {
        (entry.get('id') or entry.get('link')): entry
        for entry in recent_articles if (entry.get('id') or entry.get('link'))
    }
    
    feed_state = load_feed_state()
    is_initial_check = feed_id not in feed_state
    
    if is_initial_check:
        # On first check, we only want to post the single latest article
        # but we need to seed the memory with ALL recent articles.
        all_recent_ids = list(article_id_map.keys())
        filter_and_update_sent_articles(all_recent_ids)
        
        # We now know only the latest should be posted.
        articles_to_post = [recent_articles[0]] if recent_articles else []
        print(f"Initial check for '{feed_url}'. Seeding memory and posting 1 newest article.")
    else:
        # On subsequent checks, let the atomic function figure out what's new
        newly_found_ids = filter_and_update_sent_articles(list(article_id_map.keys()))
        articles_to_post = [article_id_map[id] for id in newly_found_ids]
        
    # Post the new articles
    if articles_to_post:
        # Sort them by date to post oldest-new first
        articles_to_post.sort(key=lambda x: x.get('published_parsed', (0,)*9))
        for entry in articles_to_post:
            title = entry.get('title', 'No Title')
            link = entry.get('link', '')
            summary = entry.get('summary', 'No summary available.')
            
            import re
            summary = re.sub('<[^<]+?>', '', summary).strip()
            if len(summary) > 250:
                summary = summary[:247] + "..."

            embed = {
                "title": title, "url": link, "description": summary, "color": 5814783,
                "footer": {"text": f"From: {feed_config.get('name', feed_url)}"}
            }

            webhooks = feed_config.get('webhooks', [])
            for webhook in webhooks:
                webhook_url = webhook.get("url")
                if webhook_url:
                    last_post_status = send_to_webhook(webhook_url, embed)
    
    return status_code, last_post_status

# --- Main Scheduler Class ---

class FeedScheduler:
    def __init__(self, interval=60):
        self.interval = interval

    def run(self):
        print("Scheduler started.")
        while True:
            print("Scheduler running check...")
            config = load_config()
            feed_state = load_feed_state()
            now = datetime.now(timezone.utc)
            
            for feed_config in config.get("FEEDS", []):
                feed_id = feed_config.get("id")
                if not feed_id: continue

                last_checked_str = feed_state.get(feed_id, {}).get('last_checked')
                update_interval = feed_config.get("update_interval", 300)

                should_check = True
                if last_checked_str:
                    last_checked = datetime.fromisoformat(last_checked_str)
                    if now - last_checked < timedelta(seconds=update_interval):
                        should_check = False
                
                if should_check:
                    print(f"Checking feed: {feed_config.get('url')}")
                    
                    if feed_id not in feed_state:
                        feed_state[feed_id] = {}

                    try:
                        status_code, last_post_status = check_single_feed(feed_config)
                        
                        feed_state[feed_id]['status_code'] = status_code
                        feed_state[feed_id]['last_checked'] = now.isoformat()
                        
                        if last_post_status:
                            feed_state[feed_id]['last_post'] = {
                                "status": last_post_status,
                                "timestamp": now.isoformat()
                            }
                        
                        save_feed_state(feed_state)
                    except Exception as e:
                        print(f"An unexpected error occurred while checking feed {feed_config.get('url')}: {e}")
                    
                    time.sleep(2)
            
            time.sleep(self.interval)

if __name__ == "__main__":
    # This main guard is mostly for direct testing, not for production with systemd
    scheduler = FeedScheduler()
    scheduler.run()
