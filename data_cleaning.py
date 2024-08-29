from google.colab import drive
drive.mount ('/content/gdrive')
from google.colab import files
from google.colab import auth
import gspread
from oauth2client.service_account import ServiceAccountCredentials
!pip install gspread_dataframe
from gspread_dataframe import set_with_dataframe

import datetime as dt
import numpy as np
import os
import pandas as pd

json_cred = '/content/gdrive/My Drive/Iteration 3/3 Calculations/Google Service Account Credentials/evr-data-cleaning-colab.json'

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(json_cred, scope)
client = gspread.authorize(creds)

raw_data_gsheet = 'https://docs.google.com/spreadsheets/d/1x600TwYFEFU8TEILsErHo3b5ePFKGv8tmTSRRJbHGsg/edit?gid=1615397883#gid=1615397883'

"""##Step 1: Retrieve and join all response data and melt into singular data frame"""

def melt_data_to_dataframe(sheet_url, sheets_to_include):
  workbook = client.open_by_url(raw_data_gsheet)
  sheets = workbook.worksheets()

  list_df = []
  for sheet in sheets:
    for s in sheets_to_include:
      if sheet.title == s:
        data = sheet.get_all_values()
        sheet_headers = data[0]
        sheet_dim = sheet_headers[0:3]
        ethnic_groups = sheet_headers[3:-2]

        for row in data[1:]:
          police_force, year, month = row[:3]
          counts = row[3:-2]
          for i, count in enumerate(counts):
            list_df.append([police_force, year, month, ethnic_groups[i], count])
        print(f"{sheet.title} append complete")

  melted_df = pd.DataFrame(list_df, columns=['Police Force', 'Year', 'Month', 'Ethnicity', 'Count'])

  # drop rows where count value is empty
  melted_df = melted_df[melted_df['Count'] != '']

  # enforce count column to be integer
  melted_df['Count'] = [int(i) for i in melted_df['Count']]

  return melted_df

# create list of tabs to include in data consolidation
workbook = client.open_by_url(raw_data_gsheet)
sheets = workbook.worksheets()

exclude_sheet_keywords = ['_draft', '_year', 'mapping', 'goal', 'ethnic', 'updateme', 'comments', 'consolidation']
include_list = []

for sheet in sheets:
  if any(t in sheet.title.lower() for t in exclude_sheet_keywords):
    continue
  include_list.append(sheet.title)

consolidated_data = melt_data_to_dataframe(raw_data_gsheet, include_list)

"""# Step 2: Melt all responses into monthly or yearly data frame"""

consolidated_data.info()

# Create table for monthly data, excludes responses that do not provide monthly berakdown

df_month = consolidated_data[(consolidated_data['Month'].str.lower() != 'na') & (consolidated_data['Count'] != '')]
# df_month['Count'] = [int(i) for i in df_month['Count']]

def standardise_month(month):
  try:
    standardise_month = pd.to_datetime(f"1900-{month}-01")
  except ValueError:
    standardise_month = month
  return standardise_month.strftime("%m")

df_month['Month'] = df_month['Month'].apply(standardise_month)
df_month

# Create table for yearly data, aggregating monthly data if available or directly importing yearly data

df_year = consolidated_data.groupby(['Police Force', 'Year', 'Ethnicity'])['Count'].sum().reset_index()
df_year

"""## Step 3: Write new aggregated data into Google Sheet

"""

def clear_and_write_to_sheet(df, sheet_url, worksheet_name):
    workbook = client.open_by_url(sheet_url)
    """ Write new aggregated back to the same Google Sheet"""

    worksheet = workbook.worksheet(worksheet_name)
    # Clear worksheet everytime before writing onto it again
    worksheet.clear()
    set_with_dataframe(worksheet, df, include_index=True, include_column_header=True, resize=True)

# Write MONTHLY aggregated data into Google Sheets
worksheet_name = 'monthly_consolidation'
clear_and_write_to_sheet(df_month, raw_data_gsheet, worksheet_name)

# Write YEARLY aggregated data into Google Sheets
worksheet_name = 'yearly_consolidation'
clear_and_write_to_sheet(df_year, raw_data_gsheet, worksheet_name)

"""## Data Validation"""

sum_raw_response_all = consolidated_data['Count'].sum()
sum_raw_response_month = consolidated_data[(consolidated_data['Month'].str.lower() != 'na') & (consolidated_data['Month'] != '')]['Count'].sum()
year_consol_total = df_year['Count'].sum()
month_consol_total = df_month['Count'].sum()

# Check total number of unique police departments tally between raw data and df_month and df_year
def count_sheets(sheet_url, exclude_sheet_keywords):
    workbook = client.open_by_url(sheet_url)
    sheets = workbook.worksheets()
    sheet_count = []
    for sheet in sheets:
      # if sheet.title not in excluded_sheet_names:
      if not any(t in sheet.title.lower() for t in exclude_sheet_keywords):
        sheet_police = (sheet.title).split('_')[1]
        sheet_count.append(sheet_police)
    return(len(sheet_count))

sheet_url = raw_data_gsheet
exclude_sheet_keywords = ['_draft', '_year', 'mapping', 'goal', 'ethnic', 'updateme', 'comments', 'consolidation']
no_of_police_force = count_sheets(sheet_url, exclude_sheet_keywords)

count_year_police = df_year['Police Force'].nunique() # check unique number of police departments in df_year
count_month_police_p1 = df_month['Police Force'].nunique() # check unique number of police departments in df_month
count_month_police_p2 = consolidated_data[(consolidated_data['Month'].str.lower() == 'na') | (consolidated_data['Month'] == '')]['Police Force'].nunique() # check unique number of police departments without monthly data/not in df_month
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
