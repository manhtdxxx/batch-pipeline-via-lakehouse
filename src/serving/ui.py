import streamlit as st
import pandas as pd
import numpy as np
import requests
import logging
import plotly.graph_objects as go
from trino_utils import TrinoUtils

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

API_URL = "http://api:8000/predict"
FEATURES = [
    "return", "body_ratio", "upper_ratio", "lower_ratio", "is_green",
    "ema_10_dist_pct", "ema_20_dist_pct", "rsi_14_scaled", "rvol_10",
]
TIMESTEPS = 10

trino = TrinoUtils(catalog="iceberg", schema="gold")
st.set_page_config(layout="wide")

# --------------------------------------------------
# SIDEBAR
# --------------------------------------------------
st.sidebar.markdown(
    """
    <div style="
        background-color: rgba(255,255,255,0.06);
        padding:14px;
        border-radius:10px;
        margin-bottom:12px;
        border: 1px solid rgba(255,255,255,0.15);
    ">
        <div style="font-size:20px;font-weight:700;color:#ffffff;">
            📊 Stock AI Dashboard
        </div>
        <div style="font-size:13px;color:rgba(255,255,255,0.75);">
            Short-term trend prediction
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

st.sidebar.title("Filter")

df_company = trino.read_table("iceberg.gold.dim_company")
symbols = sorted(df_company["symbol"].dropna().unique().tolist())

selected_symbol = st.sidebar.selectbox("Select symbol", symbols)

company_name = df_company.loc[
    df_company["symbol"] == selected_symbol,
    "company_name"
].values[0]

# --------------------------------------------------
# READ FEATURE DATA (MODEL INPUT)
# --------------------------------------------------
df_feature = trino.read_table(
    table="iceberg.gold.fact_daily_ohlcv",
    columns=["date", *FEATURES],
    filters=[f"symbol = '{selected_symbol}'"],
    order_by="date DESC",
    limit=TIMESTEPS
)

if df_feature.empty:
    st.error("No feature data for this symbol")
    st.stop()

df_feature["date"] = pd.to_datetime(df_feature["date"])
df_feature = df_feature.sort_values("date")

X = df_feature[FEATURES].values

if X.shape[0] < TIMESTEPS:
    pad = np.zeros((TIMESTEPS - X.shape[0], len(FEATURES)))
    X = np.vstack([pad, X])

payload = {"inputs": [X.tolist()]}

# --------------------------------------------------
# CALL PREDICTION API
# --------------------------------------------------
with st.spinner("Predicting..."):
    try:
        res = requests.post(API_URL, json=payload, timeout=10)
        res.raise_for_status()
        probs = res.json()["predictions"][0]
    except Exception as e:
        st.error(f"Prediction API failed: {e}")
        st.stop()

# --------------------------------------------------
# READ OHLC DATA
# --------------------------------------------------
df_ohlc = trino.read_table(
    table="iceberg.gold.fact_daily_ohlcv",
    columns=["date", "open", "high", "low", "close"],
    filters=[f"symbol = '{selected_symbol}'"]
)

if df_ohlc.empty:
    st.error("No OHLC data for this symbol")
    st.stop()

df_ohlc["date"] = pd.to_datetime(df_ohlc["date"])

for col in ["open", "high", "low", "close"]:
    df_ohlc[col] = pd.to_numeric(df_ohlc[col], errors="coerce")

df_ohlc = df_ohlc.dropna(subset=["open", "high", "low", "close"])
df_ohlc = df_ohlc.sort_values("date")

if df_ohlc.empty:
    st.error("All OHLC rows are NULL after cleaning")
    st.stop()

# --------------------------------------------------
# TITLE
# --------------------------------------------------
st.markdown(
    f"<h1 style='text-align:center'>{company_name} ({selected_symbol})</h1>",
    unsafe_allow_html=True
)

# --------------------------------------------------
# KPI
# --------------------------------------------------
st.markdown("<br>", unsafe_allow_html=True)

def kpi_box(label, value, bg_color):
    return f"""
    <div style="
        background-color:{bg_color};
        border-radius:12px;
        padding:18px;
        text-align:center;
    ">
        <div style="font-weight:600;font-size:24px;margin-bottom:6px;">
            {label}
        </div>
        <div style="font-weight:600;font-size:32px;">
            {value:.2f}%
        </div>
    </div>
    """

left, center, right = st.columns([1, 6, 1])
with center:
    c1, c2, c3 = st.columns(3)
    c1.markdown(kpi_box("Down", probs[0]*100, "rgba(231,76,60,0.4)"), unsafe_allow_html=True)
    c2.markdown(kpi_box("Sideway", probs[1]*100, "rgba(200,200,200,0.4)"), unsafe_allow_html=True)
    c3.markdown(kpi_box("Up", probs[2]*100, "rgba(46,204,113,0.4)"), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# --------------------------------------------------
# PLOTLY CANDLESTICK (ZOOM / PAN)
# --------------------------------------------------
df_ohlc = df_ohlc.sort_values("date").reset_index(drop=True)
fig = go.Figure(
    data=[
        go.Candlestick(
            x=df_ohlc["date"],
            open=df_ohlc["open"],
            high=df_ohlc["high"],
            low=df_ohlc["low"],
            close=df_ohlc["close"],
            increasing_line_color="#2ecc71",
            decreasing_line_color="#e74c3c",
        )
    ]
)

fig.update_layout(
    height=600,
    margin=dict(l=20, r=20, t=20, b=20),
    xaxis=dict(
        rangeslider=dict(visible=True),
        type="date"
    ),
    yaxis=dict(fixedrange=False),
    dragmode="pan",
    template="plotly_dark"
)

st.plotly_chart(fig, use_container_width=True)


st.write(df_ohlc.head())
st.write(df_ohlc.dtypes)
