import streamlit as st
import pandas as pd
import numpy as np
import requests
from datetime import date
import logging
from trino_utils import TrinoUtils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# --- Config ---
API_URL = "http://api:8000/predict"
FEATURES = [
    'return', 'body_ratio', 'upper_ratio', 'lower_ratio', 'is_green', 
    'ema_10_dist_pct', 'ema_20_dist_pct', 'rsi_14_scaled', 'rvol_10'
]
TIMESTEPS = 10

trino_utils = TrinoUtils(schema="gold")

# --- Sidebar Filters ---
st.sidebar.title("Filters")

# Company / Industry
dim_company = trino_utils.read_table("iceberg.gold.dim_company")
company_options = dim_company['company_name'].tolist()
selected_company = st.sidebar.selectbox("Select Company", ["All"] + company_options)

# Ngành (Industry) theo ICB code
icb_options = dim_company['icb_name'].unique().tolist()
selected_icb = st.sidebar.selectbox("Select Industry", ["All"] + icb_options)

# Start / End date
start_date = st.sidebar.date_input("Start date", value=date(2025, 1, 1))
end_date = st.sidebar.date_input("End date", value=date(2025, 12, 31))

# --- Prepare filters ---
filters = []

# Filter by company
if selected_company != "All":
    symbol = dim_company.loc[dim_company['company_name'] == selected_company, 'symbol'].values[0]
    filters.append(f"symbol = '{symbol}'")

# Filter by industry
if selected_icb != "All":
    icb_symbols = dim_company[dim_company['icb_name'] == selected_icb]['symbol'].tolist()
    icb_filter = ", ".join([f"'{s}'" for s in icb_symbols])
    filters.append(f"symbol IN ({icb_filter})")

# Filter by date range
filters.append(f"date >= DATE '{start_date}'")
filters.append(f"date <= DATE '{end_date}'")

# --- Fetch Data from fact table ---
df = trino_utils.read_table(
    table="iceberg.gold.fact_daily_ohlcv",
    columns=FEATURES + ["symbol", "date"],
    filters=filters
)

st.success(f"Loaded {len(df)} rows")
st.dataframe(df.head(10))

# --- Prepare sequences for LSTM ---
grouped = df.groupby("symbol")
inputs = []
symbols = []

for symbol, group in grouped:
    group = group.sort_values("date")
    seq = group[FEATURES].values[-TIMESTEPS:]
    if seq.shape[0] < TIMESTEPS:
        pad = np.zeros((TIMESTEPS - seq.shape[0], len(FEATURES)))
        seq = np.vstack([pad, seq])
    inputs.append(seq.tolist())
    symbols.append(symbol)

st.write(f"Prepared {len(inputs)} sequences for prediction")

# --- Call API ---
payload = {"inputs": inputs}
with st.spinner("Calling prediction API..."):
    try:
        response = requests.post(API_URL, json=payload)
        response.raise_for_status()
        preds = response.json()["predictions"]
    except Exception as e:
        st.error(f"Prediction failed: {e}")
        st.stop()

pred_df = pd.DataFrame({"symbol": symbols, "prediction": preds})
st.success("Predictions completed!")
st.dataframe(pred_df)
