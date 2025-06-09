import configparser
import sys
import telegram
import pandas as pd
from datetime import datetime, timedelta
import requests
import io

# --- Configuration ---
def load_config():
    """Loads all API keys and chat IDs from config.ini"""
    config = configparser.ConfigParser()
    config.read('config.ini')
    if 'telegram' not in config or 'alpha_vantage' not in config or 'finnhub' not in config:
        raise KeyError("Config file must contain [telegram], [alpha_vantage], and [finnhub] sections.")
    
    chat_ids_str = config['telegram'].get('chat_ids')
    if not chat_ids_str:
        raise ValueError("'chat_ids' not found or is empty in config.ini")
    
    chat_ids = [item.strip() for item in chat_ids_str.split(',')]
    
    return {
        'telegram_token': config['telegram']['bot_token'],
        'chat_ids': chat_ids,
        'alpha_vantage_key': config['alpha_vantage']['api_key'],
        'finnhub_key': config['finnhub']['api_key']
    }

# --- Data Fetching Functions ---
def get_alpha_vantage_ipos(api_key):
    """Gets IPO data from the Alpha Vantage API."""
    print("Fetching IPO data from Alpha Vantage...")
    url = f"https://www.alphavantage.co/query?function=IPO_CALENDAR&apikey={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text))
        df.rename(columns={'name': 'Company Name', 'ipoDate': 'IPO Date'}, inplace=True)
        print(f"Alpha Vantage found {len(df)} records.")
        return df[['symbol', 'Company Name', 'IPO Date']]
    except Exception as e:
        print(f"Could not fetch or process Alpha Vantage data. Error: {e}")
        return pd.DataFrame()

def get_finnhub_ipos(api_key, start_date, end_date):
    """Gets IPO data from the Finnhub API for a given date range."""
    print(f"Fetching IPO data from Finnhub for {start_date} to {end_date}...")
    url = f"https://finnhub.io/api/v1/calendar/ipo?from={start_date}&to={end_date}&token={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get('ipoCalendar', [])
        if not data:
            print("Finnhub found 0 records.")
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df.rename(columns={'name': 'Company Name', 'date': 'IPO Date'}, inplace=True)
        if 'symbol' not in df.columns: df['symbol'] = 'N/A'
        print(f"Finnhub found {len(df)} records.")
        return df[['symbol', 'Company Name', 'IPO Date']]
    except Exception as e:
        print(f"Could not fetch or process Finnhub data. Error: {e}")
        return pd.DataFrame()

# --- Telegram Bot Function ---
def send_ipo_update(bot, chat_ids, combined_ipos_df, mode='daily'):
    """Formats and sends the consolidated IPO list to multiple Telegram chats."""
    today = pd.to_datetime(datetime.now().date())
    message = ""
    
    if mode == 'daily':
        message = f"**IPOs for Today, {today.strftime('%A, %b %d')}**\n\n"
        if combined_ipos_df.empty:
            message += "No IPOs scheduled for today."
        else:
            for _, row in combined_ipos_df.iterrows():
                message += f"- {row['Company Name']} ({row['symbol']})\n"
    
    elif mode == 'weekly':
        next_week_end = today + timedelta(days=7)
        message = f"**Upcoming IPOs for the Week ({today.strftime('%b %d')} - {next_week_end.strftime('%b %d')})**\n\n"
        if combined_ipos_df.empty:
            message += "No IPOs found for the upcoming week."
        else:
            combined_ipos_df = combined_ipos_df.sort_values(by='IPO Date')
            # The error occurs on the next line, so the fix must happen before it
            for date, group in combined_ipos_df.groupby(combined_ipos_df['IPO Date'].dt.date):
                message += f"**{date.strftime('%A, %b %d')}**\n"
                for _, row in group.iterrows():
                    message += f"- {row['Company Name']} ({row['symbol']})\n"
                message += "\n"

    for chat_id in chat_ids:
        try:
            if len(message) > 4096:
                bot.send_message(chat_id=chat_id, text=message[:4090] + "\n...", parse_mode=telegram.ParseMode.MARKDOWN)
            else:
                bot.send_message(chat_id=chat_id, text=message, parse_mode=telegram.ParseMode.MARKDOWN)
            print(f"Message sent successfully to chat ID: {chat_id}")
        except Exception as e:
            print(f"Failed to send message to chat ID: {chat_id}. Error: {e}")

# --- Main Job Function ---
def run_job(mode):
    """Runs the main logic for either daily or weekly reports."""
    print(f"Running consolidated IPO check in '{mode}' mode...")
    config = load_config()
    
    today = datetime.now()
    if mode == 'daily':
        start_date_str = end_date_str = today.strftime('%Y-%m-%d')
    else: # weekly
        start_date_str = today.strftime('%Y-%m-%d')
        end_date_str = (today + timedelta(days=7)).strftime('%Y-%m-%d')
        
    av_ipos = get_alpha_vantage_ipos(config['alpha_vantage_key'])
    fh_ipos = get_finnhub_ipos(config['finnhub_key'], start_date_str, end_date_str)

    combined_df = pd.concat([av_ipos, fh_ipos], ignore_index=True) if not (av_ipos.empty and fh_ipos.empty) else pd.DataFrame()

    if not combined_df.empty:
        combined_df['IPO Date'] = pd.to_datetime(combined_df['IPO Date'], errors='coerce')
        
        # --- THE FIX ---
        # Remove any rows where the date conversion failed (resulting in NaT)
        combined_df.dropna(subset=['IPO Date'], inplace=True)
        
        combined_df.drop_duplicates(subset=['symbol'], keep='first', inplace=True)
        
        start_date = pd.to_datetime(start_date_str)
        end_date = pd.to_datetime(end_date_str)
        final_df = combined_df[(combined_df['IPO Date'] >= start_date) & (combined_df['IPO Date'] <= end_date)]
        
        print(f"Total unique IPOs for the period: {len(final_df)}")
    else:
        final_df = pd.DataFrame()
    
    bot = telegram.Bot(token=config['telegram_token'])
    send_ipo_update(bot, config['chat_ids'], final_df, mode=mode)

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    day_of_week = datetime.now().weekday()
    
    if day_of_week >= 5: # Saturday (5) or Sunday (6)
        run_job(mode='weekly')
    else: # Weekday (0-4)
        run_job(mode='daily')
        
    print("IPO check complete. Exiting.")