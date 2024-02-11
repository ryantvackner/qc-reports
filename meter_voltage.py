# -*- coding: utf-8 -*-
"""
Meter Voltage Power BI

Created on Tue Jun 13 10:32:00 2023

@author: rvackner
"""
import os
import pandas as pd
import pyodbc
from datetime import datetime, timedelta

# path of voltage files
path = r"XXXXX"

# create a list of files in the directory
files = os.listdir(path)

# the number of days you want to look back
# right now it is set for one full week
number_of_days = 6

# set the starting date
date = (datetime.today() - timedelta(days = number_of_days))

# initailize the week_prior and iteration variable
week_prior = []
w = 0

# iterate over the number of days
while w <= number_of_days:
    # append the All Voltage files from previous amount of days chosen
    week_prior.append("All Voltages__" + date.strftime('%Y-%m-%d'))
    # move on to the next day
    date = date + timedelta(days = 1)
    w += 1

# store all the correct files we are looking for in a separate list
one_week_voltage_files = [f for f in files if f[:24] in week_prior]

# initialize the dataframe
df = pd.DataFrame()

# read each excel files and concatonate them all into one datafrane
for f in one_week_voltage_files:
    data = pd.read_csv(path + "\\" + f)
    df = pd.concat([df, data])
   
# Using a DSN, but providing a password as well
cnxn = pyodbc.connect('DSN=XXXXX;PWD=XXXXX')
df_rdg = pd.read_sql_query(f"SELECT * FROM XXXXX.XXXXX ", cnxn)
   
# create a subset df to manipulate
df_sub = df[["Premise ID", "% of Nominal", "Voltage"]]

# filter by if percent of nominal is +/-5%
df_sub = df_sub[(df_sub["% of Nominal"] > 105) | (df_sub["% of Nominal"] < 95)]

# groupby premise id
df_gp = df_sub.groupby(["Premise ID"]).agg(percent_of_nominal=('% of Nominal', 'mean'), average_voltage=('Voltage', 'mean'), count=('% of Nominal', 'count')).reset_index()


# filter df rdg
df_rdg = df_rdg[["BI_ACCT", "BI_SRV_LOC_NBR", "BI_MTR_NBR", "BI_MTR_POS_NBR", "BI_PREV_MTR_RDG", "BI_PREV_READ_DT", "BI_MTR_MULT", "BI_DMD_MULT", "BI_RATE_SCHED"]]

# convert to numbers
df_rdg[["BI_MTR_NBR", "BI_RATE_SCHED"]] = df_rdg[["BI_MTR_NBR", "BI_RATE_SCHED"]].apply(pd.to_numeric, errors='coerce')


# export as scv for power bi
df_gp.to_csv(r'XXXXX\mtr_voltage_count.csv', index=False)
df_rdg.to_csv(r'XXXXX\mtr_voltage_rdg.csv', index=False)
