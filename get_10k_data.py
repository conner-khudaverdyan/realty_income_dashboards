import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import openpyxl
import os
import re

def get_filings(cik, headers):
    base_url = "https://data.sec.gov/submissions/CIK{cik}.json"
    response = requests.get(base_url.format(cik=cik), headers = headers)
    if response.status_code == 200:
        data = response.json()
        print('Success')
    else:
        print("Error:", response.status_code)
        
    filings = pd.DataFrame(data['filings']['recent'])
    return filings


def get_document(filings, primaryDocDescription, reportDate):
    selected_document = filings[(filings['primaryDocDescription'] == primaryDocDescription) & (filings['reportDate'] == reportDate)]
    accessionNumber = selected_document['accessionNumber'].iloc[0]
    primaryDoc = selected_document['primaryDocument'].iloc[0]

    formattedAccessionNumber = accessionNumber.replace("-", "")
    formattedPrimaryDoc = primaryDoc.replace("-", "")
    url = f'https://www.sec.gov/Archives/edgar/data/726728/{formattedAccessionNumber}/{primaryDoc}'
    
    response = requests.get(url, headers =headers )

    if response.status_code == 200:
        with open("o-20231231.htm", "wb") as file:
            file.write(response.content)
        print("File downloaded successfully!")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")
    return response.content
        
def get_raw_table(tables, index):
    # Extract rows from the table
    rows = tables[index].find_all('tr')

    # Initialize an empty list to hold the data
    data = []

    # Iterate through the rows and process the columns
    for row in rows:
        columns = row.find_all('td')
        column_data = [column.get_text(strip=True) for column in columns]

        
        # Filter out empty strings and other excess characters
        column_data = [item for item in column_data if item not in ['$','%', '']] 
        if column_data:
            data.append(column_data)
    return data

def save_clean_tables(cleaned_tables, table_names):
    realty_income_wb = 'stock_data/realty_income.xlsx' # workbook containing sheets with realty_income specific data
    with pd.ExcelWriter(realty_income_wb, mode='a', engine='openpyxl') as writer:
        for table, table_name in zip(cleaned_tables, table_names):
            sheet_name = table_name  
            table.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f'Saved {sheet_name} to {realty_income_wb}')

####################################################################

cik = '0000726728' # Realty Income CIK 

headers = {
    "User-Agent": "ConnerKhudaverdyan (connerkhudaverdyan@gmail.com)"  # Replace with your name and email
}

# Fetch filings        
filings = get_filings(cik, headers) 

# Fetch document using information from filings
document = get_document(filings, '10-K', '2023-12-31')

# Use Beautiful Soup to help with parising document (which is in form of htm file) 
soup = BeautifulSoup(document, 'html.parser') 

# Extract tables
tables = soup.find_all('table', {'style': 'border-collapse:collapse;display:inline-table;margin-bottom:5pt;vertical-align:text-bottom;width:100.000%'})

# Top 10 Industry Concentrations Table
ind_conc = get_raw_table(tables, 3)
dates = ind_conc[2]
years = [re.search(r'\d{4}', date).group() for date in dates] # Extract years from row of dates
columns = ['Industry'] + years
ind_df = pd.DataFrame(data = ind_conc[4:], columns = columns)

# Property Type Composition Table:  information about property portfolio goruped by Property Type
ptc = get_raw_table(tables, 4)
ptc_columns = ptc[0]
ptc_data = ptc[1:]
ptc_df = pd.DataFrame(data = ptc_data, columns = ptc_columns)


# Client Diversification Table: information about the 20 largest clients in O's portfolio
client_div = get_raw_table(tables, 5)
client_div_columns = client_div[0]
client_div_data = client_div[1:]
client_div_df = pd.DataFrame(data = client_div_data, columns = client_div_columns)


# Lease Expiration table
lease_exp = get_raw_table(tables, 6)
lease_exp_columns = lease_exp[2] + lease_exp[1][1:]
lease_exp_data = lease_exp[3:]
lease_exp_df = pd.DataFrame(data = lease_exp_data, columns = lease_exp_columns)


cleaned_tables = [ind_df, ptc_df, client_div_df, lease_exp_df]
table_names = ['Top 10 Industry Concentrations', 'Property Type Composition', 'Client Diversification', 'Lease Expiration']

# Save the cleaned_tables into realty_income.xlsx workbook 
save_clean_tables(cleaned_tables = cleaned_tables, table_names = table_names)