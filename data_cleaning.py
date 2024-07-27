# Environment Set Up 
from google.colab import drive
drive.mount ('/content/gdrive')
from google.colab import files
from google.colab import auth
import gspread
from oauth2client.service_account import ServiceAccountCredentials
!pip install gspread_dataframe
from gspread_dataframe import set_with_dataframe

import datetime as dt
import pandas as pd
import numpy as np
import os

# Google Service Account connection
json_cred = '/content/gdrive/My Drive/Iteration 3/3 Calculations/Google Service Account Credentials/evr-data-cleaning-colab.json' # Change to your own JSON here

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(json_cred, scope)
client = gspread.authorize(creds)
gsheet_containing_data = 'https://docs.google.com/spreadsheets/d/11WsLfGWv086CNEKAB5tJkakBwxIYsfx_a8w7w1ohIf4/edit#gid=1210689496' # Change URL here from testing to actual sheet

def melt_data_to_dataframe(sheet_url, excluded_sheet_names): 
    workbook = client.open_by_url(sheet_url)
    sheets = workbook.worksheets()
    """ Takes all data in sheets containing response data and return them in a DataFrame """
    # Create empty DataFrame to append data later
    melted_df = pd.DataFrame(columns=['Police Force', 'Year', 'Month', 'Ethnicity', 'Count'])

    for sheet in sheets:
        if sheet.title in excluded_sheet_names:
            continue  # Skip excluded sheets

        data = sheet.get_all_values()
        headers = data[0]
        ethnic_groups = headers[4:]

        for row in data[1:]:  # Skip headers row
            police_force, year, month = row[:3]
            for i, count in enumerate(row[4:]):  
                new_row = {'Police Force': police_force, 'Year': year, 'Month': month,
                           'Ethnicity': ethnic_groups[i], 'Count': count}
                melted_df = melted_df.append(new_row, ignore_index=True)

    return melted_df

sheet_url = gsheet_containing_data
excluded_sheet_names = ['READ/UPDATEME', 'goal', 'colnames-ethnicity', 'ethnicity-tagging', 'Consolidation', 'Jamie_Dorset'] # To remove 'Jamie_Dorest
melted_df = melt_data_to_dataframe(sheet_url, excluded_sheet_names)
melted_df['Count'] = pd.to_numeric(melted_df['Count'], errors='coerce').fillna(0).astype(int)

# Create MONTHLY DATA table, excludes responses that do not provide monthly berakdown
df_month = melted_df[(melted_df['Month'].str.lower() != 'na') & (melted_df['Count'] != '')]

def standardise_month(month):
  try:
    standardise_month = pd.to_datetime(f"1900-{month}-01")
  except ValueError:
    standardise_month = month
  return standardise_month.strftime("%m")

df_month['Month'] = df_month['Month'].apply(standardise_month)

# Create YEARLY DATA table, excludes responses that do not provide monthly berakdown
df_year = melted_df.groupby(['Police Force', 'Year', 'Ethnicity'])['Count'].sum().reset_index()

# Write new tables into Google Sheet
def clear_and_write_to_sheet(df, sheet_url, worksheet_name):
    workbook = client.open_by_url(sheet_url)
    """ Write new aggregated back to the same Google Sheet"""

    worksheet = workbook.worksheet(worksheet_name)
    # Clear worksheet everytime before writing onto it again
    worksheet.clear()
    set_with_dataframe(worksheet, df, include_index=True, include_column_header=True, resize=True)


sheet_url = gsheet_containing_data

# Write MONTHLY aggregated data into Google Sheets
worksheet_name = 'Monthly_Consolidation'
clear_and_write_to_sheet(df_month, sheet_url, worksheet_name)

# Write YEARLY aggregated data into Google Sheets
worksheet_name = 'Yearly_Consolidation'
clear_and_write_to_sheet(df_year, sheet_url, worksheet_name)


# Data Validation
# Check yearly and monthly aggregates between raw data and df_month and df_year tally
sum_raw_response_all = melted_df['Count'].sum()
sum_raw_response_month = melted_df[(melted_df['Month'].str.lower() != 'na') & (melted_df['Month'] != '')]['Count'].sum()
year_consol_total = df_year['Count'].sum()
month_consol_total = df_month['Count'].sum()


# Check total number of unique police departments tally between raw data and df_month and df_year
def count_sheets(sheet_url, excluded_sheet_names):
    workbook = client.open_by_url(sheet_url)
    sheets = workbook.worksheets()
    sheet_count = []
    for sheet in sheets:      
      if sheet.title not in excluded_sheet_names:
        sheet_police = (sheet.title).split('_')[1]
        sheet_count.append(sheet_police)
    return(len(sheet_count))

sheet_url = gsheet_containing_data
excluded_sheet_names = ['READ/UPDATEME', 'goal', 'colnames-ethnicity', 'ethnicity-tagging', 'Monthly_Consolidation', 'Yearly_Consolidation', 'Jamie_Dorset', 'Jamie_Humberside', 'Jamie_Kent'] # To remove 'Jamie_Dorest
no_of_police_force = count_sheets(sheet_url, excluded_sheet_names)

count_year_police = df_year['Police Force'].nunique() # check unique number of police departments in df_year
count_month_police_p1 = df_month['Police Force'].nunique() # check unique number of police departments in df_month
count_month_police_p2 = melted_df[(melted_df['Month'].str.lower() == 'na') | (melted_df['Month'] == '')]['Police Force'].nunique() # check unique number of police departments without monthly data/not in df_month
count_month_police = count_month_police_p1 + count_month_police_p2

print(f"Total responses from raw data: {sum_raw_response_all}")
print(f"Total responses aggregated in df_year: {year_consol_total}")
print(f"Total responses from raw data with monthly breakdown: {sum_raw_response_month}")
print(f"Total responses aggregated in df_month: {month_consol_total}")
print('\n')
if sum_raw_response_all == year_consol_total and sum_raw_response_month == month_consol_total:
  print("Yearly and monthly aggregation checks: Pass")
else: 
  print("Yearly and monthly aggregation checks: Fail")

print('\n')

print(f"Total no. of police departments from raw data: {no_of_police_force}")
print(f"Total no. of police departments from yearly data: {count_year_police}")
print(f"Total no. of police departments from monthly data: {count_month_police_p1}")
print(f"Total no. of police departments without monthly data breakdown: {count_month_police_p2}")
print('\n')
if no_of_police_force == count_year_police & no_of_police_force == count_month_police:
  print("Unique count of police forces checks: Pass")
else:
  print("Unique count of police forces checks: Fail")
