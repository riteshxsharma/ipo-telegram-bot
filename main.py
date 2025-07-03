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
def get_ipo_data(config):
    """Fetches, combines, and cleans IPO data from all sources."""
    print("Fetching all IPO data...")
    today = datetime.now()
    future_date = today + timedelta(days=30)
    
    av_ipos = get_alpha_vantage_ipos(config['alpha_vantage_key'])
    fh_ipos = get_finnhub_ipos(config['finnhub_key'], today.strftime('%Y-%m-%d'), future_date.strftime('%Y-%m-%d'))
    
    combined_df = pd.concat([av_ipos, fh_ipos], ignore_index=True) if not (av_ipos.empty and fh_ipos.empty) else pd.DataFrame()

    if not combined_df.empty:
        combined_df['IPO Date'] = pd.to_datetime(combined_df['IPO Date'], errors='coerce')
        combined_df.dropna(subset=['IPO Date'], inplace=True)
        combined_df.drop_duplicates(subset=['symbol'], keep='first', inplace=True)
        print(f"Total unique IPOs found: {len(combined_df)}")
        return combined_df
    
    print("No IPO data found from any source.")
    return pd.DataFrame()

def get_alpha_vantage_ipos(api_key):
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

# --- Helper function for formatting a message for a specific period ---
def format_ipo_period(df, start_date, end_date, title, empty_message):
    """Filters a DataFrame for a date range and returns a formatted string."""
    # Ensure the DataFrame is not empty before proceeding
    if df.empty:
        return f"**{title}**\n_{empty_message}_\n\n"

    mask = (df['IPO Date'] >= start_date) & (df['IPO Date'] <= end_date)
    period_df = df.loc[mask]
    
    if period_df.empty:
        return f"**{title}**\n_{empty_message}_\n\n"

    message = f"**{title}**\n"
    period_df = period_df.sort_values(by='IPO Date')
    for date, group in period_df.groupby(period_df['IPO Date'].dt.date):
        message += f"_{date.strftime('%A, %b %d')}_\n"
        for _, row in group.iterrows():
            message += f"- {row['Company Name']} ({row['symbol']})\n"
        message += "\n"
    return message

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    print(f"Starting IPO report job at {datetime.now()}...")
    config = load_config()
    ipo_data = get_ipo_data(config)
    bot = telegram.Bot(token=config['telegram_token'])
    
    today = datetime.now()
    today_date = pd.to_datetime(today.date())
    day_of_week = today.weekday()
    
    message = ""

    # --- THE FIX: Check if ipo_data is empty at the very beginning ---
    if ipo_data.empty:
        print("No data retrieved from APIs. Sending simple 'no IPOs' message.")
        day_name = today.strftime('%A')
        message = f"No IPOs scheduled for today, {day_name}, {today.strftime('%b %d')}."
    else:
        # Day-of-the-week Logic
        if 0 <= day_of_week <= 2:
            day_name = today.strftime('%A')
            message = f"**IPO Report for {day_name}, {today.strftime('%b %d')}**\n\n"
            message += format_ipo_period(ipo_data, today_date, today_date, "Today's IPOs", "None for today.")
            start_of_rest_of_week = today_date + timedelta(days=1)
            end_of_week = today_date + timedelta(days=(6 - day_of_week))
            if start_of_rest_of_week <= end_of_week:
                 message += format_ipo_period(ipo_data, start_of_rest_of_week, end_of_week, "Remainder of This Week", "None for the rest of this week.")
        elif day_of_week == 3:
            message = f"**IPO Outlook for Thursday, {today.strftime('%b %d')}**\n\n"
            message += format_ipo_period(ipo_data, today_date, today_date, "Today's IPOs", "None for today.")
            fri_date = today_date + timedelta(days=1)
            sun_date = today_date + timedelta(days=3)
            message += format_ipo_period(ipo_data, fri_date, sun_date, "Remainder of This Week", "None for the rest of this week.")
            next_mon = today_date + timedelta(days=4)
            next_sun = next_mon + timedelta(days=6)
            message += format_ipo_period(ipo_data, next_mon, next_sun, "Next Week's IPOs", "None scheduled for next week yet.")
        elif day_of_week == 4:
            message = f"**IPO Report for Friday, {today.strftime('%b %d')}**\n\n"
            message += format_ipo_period(ipo_data, today_date, today_date, "Today's IPOs", "None for today.")
            next_mon = today_date + timedelta(days=3)
            next_sun = next_mon + timedelta(days=6)
            message += format_ipo_period(ipo_data, next_mon, next_sun, "Next Week's IPOs", "None scheduled for next week yet.")
        else:
            end_of_period = today_date + timedelta(days=7)
            title = f"Upcoming IPOs for the Next 7 Days ({today_date.strftime('%b %d')} - {end_of_period.strftime('%b %d')})"
            message = format_ipo_period(ipo_data, today_date, end_of_period, title, "No IPOs found for the upcoming 7 days.")

    # Send the final message to all chats
    for chat_id in config['chat_ids']:
        try:
            if len(message) > 4096:
                bot.send_message(chat_id=chat_id, text=message[:4090] + "\n...", parse_mode=telegram.ParseMode.MARKDOWN)
            else:
                bot.send_message(chat_id=chat_id, text=message, parse_mode=telegram.ParseMode.MARKDOWN)
            print(f"Message sent successfully to chat ID: {chat_id}")
        except Exception as e:
            print(f"Failed to send message to chat ID: {chat_id}. Error: {e}")
            
    print("IPO check complete. Exiting.")