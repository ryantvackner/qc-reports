# -*- coding: utf-8 -*-
"""
Unbilled Revenue

Created on Wed Aug 16 14:37:15 2023

@author: rvackner
"""

import pyodbc
import pandas as pd
import numpy as np

# get data
cnxncis = pyodbc.connect('DSN=XXXXX;PWD=XXXXX')
df_rdg = pd.read_sql_query("SELECT BI_ACCT, BI_SRV_LOC_NBR, BI_RATE_SCHED FROM XXXXX.XXXXX", cnxncis)
df_type_service = pd.read_sql_query("SELECT BI_ACCT, BI_REV_CLASS_CD, BI_SRV_STAT_CD FROM XXXXX.XXXXX", cnxncis)
df_srv_loc = pd.read_sql_query(f"SELECT BI_SRV_LOC_NBR, BI_ADDR1, BI_SRV_DESC FROM XXXXX.XXXXX", cnxncis)

# merge data
df = df_rdg.merge(df_type_service, how='left', on='BI_ACCT')
df = df.merge(df_srv_loc, how='left', on='BI_SRV_LOC_NBR')

# drop duplicates
df = df.drop_duplicates()

# drop rates and revs that are non integers and convert the rest to ints
df = df[pd.to_numeric(df['BI_RATE_SCHED'], errors='coerce').notnull()]
df = df[pd.to_numeric(df['BI_REV_CLASS_CD'], errors='coerce').notnull()]
df['BI_RATE_SCHED'] = df['BI_RATE_SCHED'].apply(np.int64)
df['BI_REV_CLASS_CD'] = df['BI_REV_CLASS_CD'].apply(np.int64)

# get a list of rates and revs and sort them
rate_sched = df['BI_RATE_SCHED'].unique()
rev_class = df['BI_REV_CLASS_CD'].unique()
rate_sched.sort()
rev_class.sort()

# only deal with active accounts
srv_stat = df['BI_SRV_STAT_CD'].unique()

# write all this to excel
# each sheet is a unique rate and rev combination
with pd.ExcelWriter(r"XXXXX\unbilled_revenue.xlsx") as writer:
    for rate in rate_sched:
        for rev in rev_class:
            df_temp = df[(df['BI_RATE_SCHED'] == rate) & (df['BI_REV_CLASS_CD'] == rev)]
            if df_temp.shape[0] > 0:
                df_temp.to_excel(writer, sheet_name=f'Rate {rate}, Rev {rev}', index=False)
            else:
                continue
