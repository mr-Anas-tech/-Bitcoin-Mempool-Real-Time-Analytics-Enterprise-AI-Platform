import os
import pandas as pd
from snowflake.connector import connect
import streamlit as st

# Ensure local data directory exists
os.makedirs("data", exist_ok=True)

# Connect to Snowflake
conn = connect(
    user=st.secrets["snowflake"]["user"],
    password=st.secrets["snowflake"]["password"],
    account=st.secrets["snowflake"]["account"],
    warehouse=st.secrets["snowflake"]["warehouse"],
    database=st.secrets["snowflake"]["database"],
    schema=st.secrets["snowflake"]["schema"],
    role=st.secrets["snowflake"]["role"]
)

# Fetch Hourly Data & Save Parquet
print("Fetching Hourly data...")
df_hourly = pd.read_sql("SELECT * FROM MEMPOOL_DB.MEMPOOL_DBT.FCT_MEMPOOL_HOURS ORDER BY 1 DESC LIMIT 500;", conn)
df_hourly.columns = [col.lower() for col in df_hourly.columns]
df_hourly.to_parquet("data/hourly_mempool_backup.parquet", index=False)
print("Saved data/hourly_mempool_backup.parquet!")

# Fetch Minutely Data & Save Parquet
print("Fetching Minutely data...")
df_minutely = pd.read_sql("SELECT * FROM MEMPOOL_DB.MEMPOOL_DBT.FCT_MEMPOOL_MINUTES ORDER BY 1 DESC LIMIT 500;", conn)
df_minutely.columns = [col.lower() for col in df_minutely.columns]
df_minutely.to_parquet("data/minutely_mempool_backup.parquet", index=False)
print("Saved data/minutely_mempool_backup.parquet!")

conn.close()
print("Backup completed successfully!")