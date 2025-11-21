from vnstock.api.listing import Listing
from vnstock.api.company import Company
from vnstock.api.financial import Finance
from vnstock.api.quote import Quote
import pandas as pd
from typing import Callable, List, Union, Optional
from datetime import datetime
import time
import os
import warnings
warnings.filterwarnings("ignore")


# HELPER FUNCTION
def get_objects(symbols: list[str], obj_type: str, source: str = "VCI") -> List[Union[Company, Quote, Finance]]:
    print(f"Creating {obj_type} objects...")
    if obj_type.lower() == "company":
        cls = Company
    elif obj_type.lower() == "quote":
        cls = Quote
    elif obj_type.lower() == "finance":
        cls = Finance
    else:
        raise ValueError(f"Unknown obj_type: {obj_type}")
    print(f"{len(symbols)} {obj_type} objects created.")
    return [cls(symbol=s, source=source) for s in symbols]


# GET SYMBOLS BY GROUP
def get_symbol_list(symbol_group:str, source: str = "VCI") -> list[str]:
    listing = Listing(source=source)
    print(f"Fetching symbols for group {symbol_group}...")
    symbols = listing.symbols_by_group(symbol_group).to_list() # returns pd.Series, then converts to list
    print(f"{len(symbols)} symbols fetched.")
    return symbols


# GET COMPANIES
def get_company_df(symbols: list[str], source: str = "VCI") -> pd.DataFrame:
    listing = Listing(source=source)
    print("Fetching companies...")
    df_company_all = listing.symbols_by_industries()
    df_company = df_company_all[df_company_all["symbol"].isin(symbols)].reset_index(drop=True)
    print(f"Companies fetched ({len(df_company_all)}), then filtered to ({len(df_company)} rows).")
    # select cols and sort by symbol
    cols_to_select = ["symbol", "organ_name", "icb_code1", "icb_code2", "icb_code3", "icb_code4"]
    df_company = df_company[cols_to_select].sort_values(by=["symbol"], ascending=[True]).reset_index(drop=True)
    return df_company


# GET INDUSTRIES
def get_industry_df(source: str = "VCI") -> pd.DataFrame:
    print("Fetching industry info...")
    listing = Listing(source=source)
    df_industry = listing.industries_icb()
    print(f"Industry info fetched ({len(df_industry)} rows).")
    # select cols and sort by code
    cols_to_select = ["icb_code", "level", "icb_name", "en_icb_name"]
    df_industry = df_industry[cols_to_select].sort_values(by=["icb_code"], ascending=[True]).reset_index(drop=True)
    return df_industry


# GET COMPANY INFO (E.G., SHAREHOLDERS, NEWS, EVENTS)
def get_company_info_df(func: Callable[[Company], pd.DataFrame], company_objects: list[Company], **kwargs) -> pd.DataFrame:
    print(f"Fetching company info via {func.__name__}...")
    dfs = []
    for c in company_objects:
        df = func(c, **kwargs)
        df["symbol"] = c.symbol
        dfs.append(df)
    dfs = pd.concat(dfs, axis=0, ignore_index=True)
    print(f"Company info via {func.__name__} fetched ({len(dfs)} rows).")
    return dfs

def get_company_shareholders_df(company_objects: List[Company]) -> pd.DataFrame:
    dfs = get_company_info_df(Company.shareholders, company_objects)
    return dfs

def get_company_officers_df(company_objects: List[Company]) -> pd.DataFrame:
    dfs = get_company_info_df(Company.officers, company_objects)
    return dfs

def get_company_events_df(company_objects: List[Company], start_date: Union[str, datetime], end_date: Optional[Union[str, datetime]] = None) -> pd.DataFrame:
    dfs = get_company_info_df(Company.events, company_objects)
    dfs = dfs[dfs["issue_date"] >= (start_date.strftime("%Y-%m-%d") if isinstance(start_date, datetime) else start_date)]
    if end_date is not None:
        dfs = dfs[dfs["issue_date"] <= (end_date.strftime("%Y-%m-%d") if isinstance(end_date, datetime) else end_date)]
    return dfs

def get_company_news_df(company_objects: List[Company], start_date: Union[str, datetime], end_date: Optional[Union[str, datetime]] = None) -> pd.DataFrame:
    dfs = get_company_info_df(Company.news, company_objects)
    dfs['public_date'] = pd.to_datetime(dfs['public_date'], unit='ms')
    dfs['public_date'] = dfs['public_date'].dt.strftime('%Y-%m-%d')

    dfs = dfs[dfs["public_date"] >= (start_date.strftime("%Y-%m-%d") if isinstance(start_date, datetime) else start_date)]
    if end_date is not None:
        dfs = dfs[dfs["public_date"] <= (end_date.strftime("%Y-%m-%d") if isinstance(end_date, datetime) else end_date)]
    return dfs


# GET OHLCV (OPEN - HIGH - LOW - CLOSE - VOLUME)
def get_ohlcv_df(quote_objects: list[Quote], start_date: Union[str, datetime], end_date: Optional[Union[str, datetime]] = None, interval: str = "1d") -> pd.DataFrame:

    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")
    if isinstance(start_date, datetime):
        start_date = start_date.strftime("%Y-%m-%d")
    if isinstance(end_date, datetime):
        end_date = end_date.strftime("%Y-%m-%d")

    print(f"Fetching OHLCV ({interval}) from {start_date} to {end_date}...")
    dfs = []
    for obj in quote_objects:
        df = obj.history(start=start_date, end=end_date, interval=interval)
        df["symbol"] = obj.symbol
        dfs.append(df)
    dfs = pd.concat(dfs, axis=0, ignore_index=True)
    print(f"OHLCV ({interval}) fetched ({len(dfs)} rows).")
    # select cols and sort by time
    cols_to_select = ["symbol", "time", "open", "high", "low", "close", "volume"]
    dfs = dfs[cols_to_select].sort_values(by=["time"], ascending=[True]).reset_index(drop=True)
    return dfs


# GET FINANCIAL RATIOS
def get_ratio_df(finance_objects: list[Finance], start_year: int, end_year: Optional[int] = None, period: str = "year", language: str ="en") -> pd.DataFrame:
    print(f"Fetching financial ratios by {period} from {start_year} to {end_year if end_year else 'latest'}...")
    dfs = []
    for obj in finance_objects:
        df = obj.ratio(period=period, lang=language)
        dfs.append(df)
    dfs = pd.concat(dfs, axis=0, ignore_index=True)
    print(f"Financial ratios by {period} fetched ({len(dfs)} rows).")
    # pick the second level or first level if second is empty from MultiIndex
    if isinstance(dfs.columns, pd.MultiIndex):
        dfs.columns = [(col[1] if col[1] != "" else col[0]).strip() for col in dfs.columns]
    # filter by year
    if end_year is None:
        dfs = dfs[dfs["yearReport"] >= start_year]
    else:
        dfs = dfs[(dfs["yearReport"] >= start_year) & (dfs["yearReport"] <= end_year)]
    # sort by year
    dfs = dfs.sort_values(by=["yearReport"], ascending=[True]).reset_index(drop=True)
    return dfs


# COMBINE ALL IN ONE FUNCTION
def get_all_data(symbol_group, start_date: str = "2020-01-01", end_date: Optional[str] = None):
    symbols = get_symbol_list(symbol_group=symbol_group)
    company_objects = get_objects(symbols, "company")
    quote_objects = get_objects(symbols, "quote")
    finance_objects = get_objects(symbols, "finance")

    # get Company & Industry
    time.sleep(60)
    df_company = get_company_df(symbols)
    df_industry = get_industry_df()

    # get Company Info
    time.sleep(60)
    df_company_shareholders = get_company_shareholders_df(company_objects)
    df_company_officers = get_company_officers_df(company_objects)
    time.sleep(60)
    df_company_events = get_company_events_df(company_objects, start_date=start_date, end_date=end_date)
    df_company_news = get_company_news_df(company_objects, start_date=start_date, end_date=end_date)

    # get OHLCV
    time.sleep(60)
    df_ohlcv_1d = get_ohlcv_df(quote_objects, start_date=start_date, end_date=end_date, interval="1D")

    # get Financial Ratios
    start_year = int(start_date.split("-")[0])
    time.sleep(60)
    df_yearly_ratio = get_ratio_df(finance_objects, start_year, period="year")
    time.sleep(60)
    df_quarterly_ratio = get_ratio_df(finance_objects, start_year, period="quarter")

    return (
        df_industry, df_company, 
        df_company_shareholders, df_company_officers, df_company_events, df_company_news, 
        df_ohlcv_1d, df_yearly_ratio, df_quarterly_ratio
    )


# EXPORT TO CSV
def export_to_csv(df: pd.DataFrame, file_name: str):
    data_folder = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(data_folder, file_name)
    print(f"Exporting {file_path}...")
    df.to_csv(file_path, index=False)
    print(f"CSV export completed: {file_path}")


# MAIN RUN
if __name__ == "__main__":
    
    symbol_group = "VN30"

    (df_industry, df_company, 
     df_company_shareholders, df_company_officers, 
     df_company_events, df_company_news, 
     df_ohlcv_1d, df_yearly_ratio, df_quarterly_ratio) = get_all_data(symbol_group, start_date="2020-01-01", end_date=None)

    export_to_csv(df_industry, "industry.csv")
    export_to_csv(df_company, "company.csv")
    export_to_csv(df_company_shareholders, "company_shareholders.csv")
    export_to_csv(df_company_officers, "company_officers.csv")
    export_to_csv(df_company_events, "company_events.csv")
    export_to_csv(df_company_news, "company_news.csv")
    export_to_csv(df_ohlcv_1d, "daily_ohlcv.csv")
    export_to_csv(df_yearly_ratio, "yearly_ratio.csv")
    export_to_csv(df_quarterly_ratio, "quarterly_ratio.csv")

    print("All Done!")
