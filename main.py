import configparser
import schedule
import time
import telegram
import pandas as pd
from datetime import datetime, timedelta
import requests
import io

# --- Configuration ---
def load_config():
    """Loads configuration from config.ini"""
    config = configparser.ConfigParser()
    config.read('config.ini')
    if 'telegram' not in config or 'alpha_vantage' not in config:
        raise KeyError("Config file must contain [telegram] and [alpha_vantage] sections.")
    
    chat_ids_str = config['telegram'].get('chat_ids')
    if not chat_ids_str:
        raise ValueError("'chat_ids' not found or is empty in config.ini")
    
    chat_ids = [item.strip() for item in chat_ids_str.split(',')]
    
    config_data = {
        'telegram_token': config['telegram']['bot_token'],
        'chat_ids': chat_ids,
        'api_key': config['alpha_vantage']['api_key']
    }
    return config_data

# --- Get IPOs from Alpha Vantage API ---
def get_ipo_data(api_key):
    """Gets IPO data using the Alpha Vantage API."""
    print("Getting IPO calendar data from Alpha Vantage API...")
    url = f"https://www.alphavantage.co/query?function=IPO_CALENDAR&apikey={api_key}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        csv_data = io.StringIO(response.text)
        ipo_df = pd.read_csv(csv_data)

        if ipo_df.empty:
            print("IPO Calendar from Alpha Vantage is empty.")
            return pd.DataFrame()

        ipo_df.rename(columns={'name': 'Company Name', 'ipoDate': 'IPO Date'}, inplace=True)
        ipo_df['IPO Date'] = pd.to_datetime(ipo_df['IPO Date'], errors='coerce')
        
        print(f"Successfully fetched {len(ipo_df)} total IPO records.")
        return ipo_df
    except Exception as e:
        print(f"An error occurred fetching or processing IPO data: {e}")
    return pd.DataFrame()

# --- Telegram Bot Function ---
def send_ipo_update(bot, chat_ids, api_key, mode='daily'):
    """Fetches IPOs and sends an update to multiple Telegram chats."""
    all_ipos = get_ipo_data(api_key)
    
    if all_ipos.empty:
        for chat_id in chat_ids:
            bot.send_message(chat_id=chat_id, text="Could not retrieve any upcoming IPO data from Alpha Vantage.")
        return

    today = pd.to_datetime(datetime.now().date())
    message = ""
    
    if mode == 'daily':
        target_ipos = all_ipos[all_ipos['IPO Date'] == today]
        message = f"**IPOs for Today, {today.strftime('%A, %b %d')}**\n\n"
        if target_ipos.empty:
            message = f"No IPOs scheduled for today, {today.strftime('%A, %b %d')}."
        else:
            for _, row in target_ipos.iterrows():
                message += f"- {row['Company Name']} ({row['symbol']})\n"

    else:  # weekly
        # --- NEW: Logic for Recently Priced and Upcoming IPOs ---
        
        # 1. Filter for Recently Priced IPOs (Past 7 Days)
        last_week_start = today - timedelta(days=7)
        recent_ipos = all_ipos[(all_ipos['IPO Date'] >= last_week_start) & (all_ipos['IPO Date'] < today)]
        
        # 2. Filter for Upcoming IPOs (Next 7 Days)
        next_week_end = today + timedelta(days=7)
        upcoming_ipos = all_ipos[(all_ipos['IPO Date'] >= today) & (all_ipos['IPO Date'] <= next_week_end)]

        # 3. Build the message string
        message = ""
        
        if not recent_ipos.empty:
            message += f"**Recently Priced (Past 7 Days)**\n"
            recent_ipos = recent_ipos.sort_values(by='IPO Date', ascending=False)
            for date, group in recent_ipos.groupby(recent_ipos['IPO Date'].dt.date):
                message += f"_{date.strftime('%A, %b %d')}_\n"
                for _, row in group.iterrows():
                    message += f"- {row['Company Name']} ({row['symbol']})\n"
            message += "\n"

        if not upcoming_ipos.empty:
            message += f"**Upcoming IPOs (Next 7 Days)**\n"
            upcoming_ipos = upcoming_ipos.sort_values(by='IPO Date')
            for date, group in upcoming_ipos.groupby(upcoming_ipos['IPO Date'].dt.date):
                message += f"_{date.strftime('%A, %b %d')}_\n"
                for _, row in group.iterrows():
                    message += f"- {row['Company Name']} ({row['symbol']})\n"
            message += "\n"
        
        if not message: # If both lists were empty
            message = "No recently priced or upcoming IPOs found in the calendar."

    # Telegram has a message length limit of 4096 characters
    if len(message) > 4096:
        message = message[:4090] + "\n..."
    
    # Loop through all chat IDs and send the final message
    for chat_id in chat_ids:
        try:
            bot.send_message(chat_id=chat_id, text=message, parse_mode=telegram.ParseMode.MARKDOWN)
            print(f"Message sent successfully to chat ID: {chat_id}")
        except Exception as e:
            print(f"Failed to send message to chat ID: {chat_id}. Error: {e}")

# --- Scheduler Job Functions ---
def scheduled_job(mode):
    print(f"Running {mode.upper()} IPO check...")
    config = load_config()
    bot = telegram.Bot(token=config['telegram_token'])
    send_ipo_update(bot, config['chat_ids'], config['api_key'], mode=mode)

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    MODE = "test_weekly"  # Or "test_daily" or "run_live"

    if MODE.startswith("test"):
        print(f"--- RUNNING IN {MODE.upper()} MODE ---")
        job_mode = "weekly" if "weekly" in MODE else "daily"
        scheduled_job(mode=job_mode)
        print("--- TEST MODE FINISHED ---")
    
    elif MODE == "run_live":
        print("--- SCHEDULER IS LIVE ---")
        schedule.every().saturday.at("10:00").do(scheduled_job, mode='weekly')
        # Add daily jobs if desired
        while True:
            schedule.run_pending()
            time.sleep(1)