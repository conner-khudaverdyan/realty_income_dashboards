import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import openpyxl
import os

api_key = 'Insert your own API Key'

# Function to fetch data from Tiingo
headers = {
        'Content-Type': 'application/json',
        'Authorization' : f'Token {api_key}'
}

## File paths
data_dir = "stock_data"
o_file = os.path.join(data_dir, 'realty_income.xlsx')
competitor_file = os.path.join(data_dir,'competitors.xlsx')

# Define Competitors
symbols = ['NNN', 'SPG', 'KIM', 'FRT', 'VNO', 'WPC', 'EPR']


# Define function to check if date formats match what fetch_historical expects
def is_valid_date_format(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

# Define function to retrieve stock data for a given time range
def fetch_historical(symbol, start, end):
    if (is_valid_date_format(start) and is_valid_date_format(end)) == False:
        print('Error, make sure start and end is YYYY-M-D format')
        return ''
    url = f'https://api.tiingo.com/tiingo/daily/{symbol}/prices?startDate={start}&endDate={end}'
    response = requests.get(url, headers = headers)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print("Error fetching data. Please check your API key or parameters.")
        return None
    

# Function to append the data to the Excel file
def update_o_file():
    # Fetch new data (here assuming we are fetching stock data)
    if os.path.exists(o_file):
        
        # Get existing data from file
        with pd.ExcelFile(o_file) as xls:
            existing_data = pd.read_excel(xls, sheet_name='O_stock_data') 
            
            # Get most recent date from existing data
            most_recent_date = existing_data.loc[existing_data.index[-1], 'date']
            most_recent_date = datetime.strptime(most_recent_date[:10], "%Y-%m-%d")

        # Get data since last update
        today = datetime.today().strftime('%Y-%m-%d')
        start_date = most_recent_date.strftime("%Y-%m-%d")
        
        # Get new data
        new_o_data = pd.DataFrame(fetch_historical('O', start_date, today))
        
        # Align columns based on the Excel sheet's columns
        new_o_data = new_o_data[existing_data.columns[0:len(existing_data.columns)-1]]
        
        # Avoid duplicate row
        new_o_data = new_o_data.iloc[1:]
        
        # If file exists, open it with pandas and append the data
        with pd.ExcelWriter(o_file, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            new_o_data.to_excel(writer, sheet_name='O_stock_data', index=False, header=False, startrow=writer.sheets['O_stock_data'].max_row)

        print(f"Appended new data to {o_file}")
    else:
        print(f'File {o_file} does not exist')

            
        
def update_competitor_files():
    # Fetch new competitor data 
    for symbol in symbols:
        competitor_sheet = f'{symbol}_stock_data'

        if os.path.exists(competitor_file):
            
            # Access Existing Data
            with pd.ExcelFile(competitor_file) as xls:
                existing_data = pd.read_excel(xls, sheet_name=competitor_sheet)  

                # Get most recent date from existing data
                most_recent_date = existing_data.loc[existing_data.index[-1], 'date']
                most_recent_date = datetime.strptime(most_recent_date[:10], "%Y-%m-%d")

            # Define start and end dates
            today = datetime.today().strftime('%Y-%m-%d')
            start_date = most_recent_date.strftime("%Y-%m-%d")
            
            # Get new data
            new_data = pd.DataFrame(fetch_historical('O', start_date, today))
            
            # Align columns based on the Excel sheet's columns
            new_data = new_data[existing_data.columns]
            
            # Avoid duplicate row
            new_data = new_data.iloc[1:]
        
            
            # If file exists, open it with pandas and append the data
            with pd.ExcelWriter(competitor_file, engine='openpyxl', mode='a',if_sheet_exists='overlay') as writer:
                new_data.to_excel(writer, sheet_name=competitor_sheet, index=False, header=False, startrow=writer.sheets[competitor_sheet].max_row)
            print(f"Appended {symbol} data to {competitor_file}")
        else:
            print(f'File {competitor_file} does not exist')
            
    print('Appended Competitor Files')

            
update_o_file()
update_competitor_files()