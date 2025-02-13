import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import openpyxl
import os

api_key = 'Insert API Key'

# Function to fetch data from Tiingo
headers = {
        'Content-Type': 'application/json',
        'Authorization' : f'Token {api_key}'
}

## File paths
dag_folder = os.path.dirname(os.path.abspath(__file__))

stock_data_dir = "/opt/airflow/stock_data"  # path inside the container
o_file = os.path.join(stock_data_dir, 'realty_income.xlsx')
competitor_file = os.path.join(stock_data_dir,'competitors.xlsx')
symbols = ['NNN', 'SPG', 'KIM', 'FRT', 'VNO', 'WPC', 'EPR']

def is_valid_date_format(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False
    
def fetch_today(symbol):
    url = f'https://api.tiingo.com/tiingo/daily/{symbol}/prices'
    response = requests.get(url, headers = headers)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print("Error fetching data. Please check your API key or parameters.")
        return None
    

def fetch_daily_data(symbol, date):
    if (is_valid_date_format(date)) == False:
        print('Error, make sure start and end is YYYY-M-D format')
        return ''
    url = f'https://api.tiingo.com/tiingo/daily/{symbol}/prices?startDate={date}&endDate={date}'
    response = requests.get(url, headers = headers)
    if response.status_code == 200:
        data = response.json()
        print(f"Successfully fetched {symbol} data from date {date}")
        return data
    else:
        print(f"Error fetching data from date {date}. Please check your API key or parameters.")
        return None
    
# Function to append the data to the Excel file
def update_o_file(**kwargs):
    # Get the execution date from the Airflow context
    execution_date = kwargs['execution_date']
    
    # Convert the execution date to a string in the required format
    date = execution_date.strftime('%Y-%m-%d')
    # Fetch new data 
    o_stock_data = fetch_daily_data(symbol="O", date =date)
    
    if o_stock_data != []: # Requests on days with no stock data such as the weekend or holidays, return an empty list
        
        # Convert the data to a DataFrame
        new_o_data = pd.DataFrame([o_stock_data[0]])

        # Check if the Excel file exists
        if os.path.exists(o_file):
            
            # Writer appends data on column order rather than name, so we have to manually match the column order of the new data to the existing data
            with pd.ExcelFile(o_file) as xls:
                existing_data = pd.read_excel(xls, sheet_name='O_stock_data', nrows=0)  # Only read the first row (headers)
                
                # Align columns based on the Excel sheet's columns
                new_o_data = new_o_data[existing_data.columns]

            # If file exists, open it with pandas and append the data
            with pd.ExcelWriter(o_file, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                new_o_data.to_excel(writer, sheet_name='O_stock_data', index=False, header=False, startrow=writer.sheets['O_stock_data'].max_row)

            print(f"Appended new data to {o_file}")
        else:
            print(f'File {o_file} does not exist')
    else:
        print(f'No new O data for {date}')
            
        
def update_competitor_file(**kwargs):
    # Get the execution date from the Airflow context
    execution_date_UTC = kwargs['execution_date']
    execution_date_PST = execution_date_UTC - timedelta(days=1) 
    
    # Convert the execution date to a string in the required format
    date = execution_date_PST.strftime('%Y-%m-%d')
    
    # Fetch new competitor data 
    for symbol in symbols:
        competitor_sheet = f'{symbol}_stock_data'
        stock_data = fetch_daily_data(symbol=symbol, date = date)
    
        if stock_data != []:
            
            # Convert the data to a DataFrame
            new_data = pd.DataFrame([stock_data[0]])
            
            # Check if the Excel file exists
            if os.path.exists(competitor_file):
                
            # Writer appends data on column order rather than name, so we have to manually match the column order of the new data to the existing data
                with pd.ExcelFile(competitor_file) as xls:
                    existing_data = pd.read_excel(xls, sheet_name=competitor_sheet, nrows=0)  # Only read the first row (headers)

                    # Align columns based on the Excel sheet's columns
                    new_data = new_data[existing_data.columns]

                
                # If file exists, open it with pandas and append the data
                with pd.ExcelWriter(competitor_file, engine='openpyxl', mode='a',if_sheet_exists='overlay') as writer:
                    new_data.to_excel(writer, sheet_name=competitor_sheet, index=False, header=False, startrow=writer.sheets[competitor_sheet].max_row)
                print(f"Appended {symbol} data to {competitor_file}")
            else:
                print(f'File {competitor_file} does not exist')
        else:
            print(f'No new {symbol} data from {date}')
            

 
with DAG(
    dag_id = 'update_stock_data',
    description='A DAG to append stock data to an Excel file daily',
    schedule='30 04 * * *', # Run daily at 8:30 pm PST, leaving for time for adjustment after trade closes
    default_args = {
        "owner" : "airflow",
        "retries" : 1,
        "retry_delay": timedelta(minutes = 5),
        "start_date" : datetime(2025, 1, 21),
    },
    catchup=True,  # Enable catchup to account for days where computer is not actively open
) as dag:

    # Define task to update realty income sheet daily
    update_o_task = PythonOperator(
        task_id='update_o_file',
        python_callable=update_o_file
    )

    # Define task to update competitor sheets daily
    update_competitor_task = PythonOperator(
        task_id='update_competitor_file',
        python_callable=update_competitor_file
    )
