# -*- coding: utf-8 -*-
"""
billing qc

Created on Fri Jun  9 11:58:23 2023

@author: rvackner
"""

import pyodbc
import pandas as pd
import numpy as np
from datetime import date
from datetime import timedelta

# todays date
today = date.today()

# Using a DSN, but providing a password as well
cnxnsql = pyodbc.connect('DRIVER=XXXXX;SERVER=XXXXX;DATABASE=XXXXX;Trusted_Connection=XXXXX')
cnxncis = pyodbc.connect('DSN=XXXXX;PWD=XXXXX')

# get dataframe of all
df_mdm_cum = pd.read_sql_query(f"SELECT * FROM XXXXX.XXXXX WHERE Recorded_Date >= CAST(DATEADD(DAY, -4, GETDATE()) AS DATE)", cnxnsql)
df_type_srv = pd.read_sql_query(f"SELECT * FROM XXXXX.XXXXX", cnxncis)
df_rdg = pd.read_sql_query(f"SELECT * FROM XXXXX.XXXXX", cnxncis)
df_int_rdg = pd.read_sql_query(f"SELECT * FROM XXXXX.XXXXX WHERE BI_INTERVAL_RDG_DT_TM >= SYSDATE - INTERVAL '13' DAY", cnxncis)
df_mtr_srv = pd.read_sql_query(f"SELECT * FROM XXXXX.XXXXX ", cnxncis)

# clear some columns from type service and readings dataframe
df_type_srv = df_type_srv[["BI_ACCT", "BI_SRV_STAT_CD"]]
df_rdg = df_rdg[["BI_ACCT", "BI_MTR_NBR", "BI_MTR_MULT", "BI_DMD_MULT", "BI_RATE_SCHED", "BI_MTR_SET_DT"]]
df_rdg = df_rdg.merge(df_type_srv, how='left', on='BI_ACCT')



# new accounts
df_mdm_cum["Recorded_Date"] = pd.to_datetime(df_mdm_cum["Recorded_Date"], errors='coerce')
df_mdm_cum_day = df_mdm_cum[df_mdm_cum["Recorded_Date"].dt.date == (today - timedelta(days = 2))]
df_rdg["BI_MTR_SET_DT"] = pd.to_datetime(df_rdg["BI_MTR_SET_DT"], errors='coerce')
df_new_accts = df_rdg[df_rdg["BI_MTR_SET_DT"].dt.date >= (today - timedelta(days = 12))]
df_new_accts = df_new_accts.merge(df_mdm_cum_day, how='left', left_on='BI_MTR_NBR', right_on='Meter_Number')
df_new_accts = df_new_accts.drop(columns=["Meter_Number", "Import_Date"])



# active accounts with no interval readings
df_int_rdg["BI_INTERVAL_RDG_DT_TM"] = pd.to_datetime(df_int_rdg["BI_INTERVAL_RDG_DT_TM"], errors='coerce')
df_int_rdg_day = df_int_rdg[df_int_rdg["BI_INTERVAL_RDG_DT_TM"].dt.date >= (today - timedelta(days = 3))]
df_type_srv_active = df_type_srv[(df_type_srv["BI_SRV_STAT_CD"] == 1) | (df_type_srv["BI_SRV_STAT_CD"] == 18)]
df_active_no_rdg = df_type_srv_active[~df_type_srv_active["BI_ACCT"].isin(df_int_rdg_day["BI_ACCT"])]
df_active_no_rdg = df_active_no_rdg.merge(df_rdg, how='left', on='BI_ACCT')
df_active_no_rdg = df_active_no_rdg.drop(columns=["BI_SRV_STAT_CD_y"])
df_active_no_rdg = df_active_no_rdg.rename(columns={'BI_SRV_STAT_CD_x': 'BI_SRV_STAT_CD'})



# stuck demand
df_stuck_dmd = df_int_rdg.groupby(['BI_ACCT', 'BI_MTR_NBR'])["BI_DMD_RDG"].agg([pd.Series.nunique, np.mean]).reset_index()
srv_use_code = df_mtr_srv[["BI_ACCT", "BI_ELEC_USE_CD"]]
srv_use_code = srv_use_code.drop_duplicates(subset=['BI_ACCT'], keep='first')
df_stuck_dmd = df_stuck_dmd.merge(srv_use_code, how="left", on="BI_ACCT")
df_stuck_dmd = df_stuck_dmd.merge(df_rdg[["BI_ACCT", "BI_MTR_MULT", "BI_DMD_MULT", "BI_RATE_SCHED", "BI_MTR_SET_DT", "BI_SRV_STAT_CD"]], how="left", on="BI_ACCT")
df_stuck_dmd = df_stuck_dmd[df_stuck_dmd['nunique'] == 1]
df_stuck_dmd = df_stuck_dmd[(df_stuck_dmd["BI_SRV_STAT_CD"] == 1) | (df_type_srv["BI_SRV_STAT_CD"] == 18)]
df_stuck_dmd = df_stuck_dmd[~((df_stuck_dmd["mean"] == 0) & ((df_stuck_dmd["BI_ELEC_USE_CD"] == 0) | (df_stuck_dmd["BI_ELEC_USE_CD"] == 1)))]



# write to csv
df_new_accts.to_csv(r"XXXXX\new_accts.csv", index=False)
df_active_no_rdg.to_csv(r"XXXXX\active_no_rdg.csv", index=False)
df_stuck_dmd.to_csv(r"XXXXX\stuck_dmd.csv", index=False)
