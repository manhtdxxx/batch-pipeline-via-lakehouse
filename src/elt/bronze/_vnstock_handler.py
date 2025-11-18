# bronze/_vnstock_handler.py

from vnstock.api.listing import Listing
from vnstock.api.company import Company
from vnstock.api.financial import Finance
from vnstock.api.quote import Quote
import pandas as pd
from datetime import datetime
from typing import Callable, List, Union
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

class VnstockHandler:
    def __init__(self, source: str = "VCI", symbol_group: str = "VN30", symbols: List[str] = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.source = source
        self.symbols = symbols if symbols is not None else self._fetch_symbol_list(symbol_group=symbol_group, source=self.source)


    # FETCH SYMBOL LIST BY GROUP
    def _fetch_symbol_list(self, symbol_group: str = "VN30", source: str = "VCI") -> List[str]:
        self.logger.info(f"Fetching symbol list for group: {symbol_group} ...")
        listing = Listing(source=source)
        symbols = listing.symbols_by_group(symbol_group).to_list()
        self.logger.info(f"Fetched {len(symbols)} symbols.")
        return symbols
    

    # FETCH COMPANY DATA
    def fetch_company_df(self) -> pd.DataFrame:
        self.logger.info(f"Fetching company data...")
        listing = Listing(source=self.source)
        df = listing.symbols_by_industries()
        df = df[df["symbol"].isin(self.symbols)].reset_index(drop=True)
        self.logger.info(f"Fetched {len(df)} companies.")

        cols_to_select = ["symbol", "organ_name", "icb_code1", "icb_code2", "icb_code3", "icb_code4"]
        return df[cols_to_select].sort_values("symbol").reset_index(drop=True)


    # FETCH INDUSTRY DATA
    def fetch_industry_df(self) -> pd.DataFrame:
        self.logger.info("Fetching industry data ...")
        listing = Listing(source=self.source)
        df = listing.industries_icb()
        self.logger.info(f"Fetched {len(df)} industries.")
        
        cols_to_select = ["icb_code", "level", "icb_name", "en_icb_name"]
        return df[cols_to_select].sort_values("icb_code").reset_index(drop=True)


    # FETCH SYMBOL OBJECTS
    def _fetch_symbol_objects(self, object_type: str) -> List[Union[Finance, Company, Quote]]:
        self.logger.info(f"Fetching {object_type} objects ...")
        if object_type.lower() == "company":
            cls = Company
        elif object_type.lower() == "quote":
            cls = Quote
        elif object_type.lower() == "finance":
            cls = Finance
        else:
            self.logger.error(f"Unknown object_type: {object_type}")
        objects = [cls(symbol=s, source=self.source) for s in self.symbols]
        self.logger.info(f"Fetched {len(objects)} {object_type} objects.")
        return objects


    # FETCH COMPANY INFO (SHAREHOLDERS, NEWS, EVENTS)
    def _fetch_company_info_df(self, func: Callable[[Company], pd.DataFrame]) -> pd.DataFrame:
        company_objects = self._fetch_symbol_objects(object_type="company")

        self.logger.info(f"Fetching company info via {func.__name__} ...")
        dfs = []
        for c in company_objects:
            df = func(c)
            df["symbol"] = c.symbol
            dfs.append(df)
        self.logger.info(f"Fetched company info via {func.__name__} ({len(dfs)} rows).")

        return pd.concat(dfs, ignore_index=True)
    

    def fetch_company_shareholders_df(self) -> pd.DataFrame:
        return self._fetch_company_info_df(Company.shareholders)
    
    def fetch_company_officers_df(self) -> pd.DataFrame:
        return self._fetch_company_info_df(Company.officers)

    def fetch_company_news_df(self, start_date: str, end_date: str = None) -> pd.DataFrame:
        df = self._fetch_company_info_df(Company.news)
        df['public_date'] = pd.to_datetime(df['public_date'], unit='ms')
        df['public_date'] = df['public_date'].dt.strftime('%Y-%m-%d')
        df = df[df["public_date"] >= start_date]
        if end_date is not None:
            df = df[df["public_date"] <= end_date]
        return df.reset_index(drop=True)

    def fetch_company_events_df(self, start_date: str, end_date: str = None) -> pd.DataFrame:
        df = self._fetch_company_info_df(Company.events)
        df = df[df["issue_date"] >= start_date]
        if end_date is not None:
            df = df[df["issue_date"] <= end_date]
        return df.reset_index(drop=True)


    # FETCH OHLCV DATA
    def fetch_ohlcv_df(self, start_date: str, end_date: str = None, interval: str = "1d") -> pd.DataFrame:
        quote_objects = self._fetch_symbol_objects(object_type="quote")

        if end_date is None:
            end_date = datetime.today().strftime("%Y-%m-%d")

        self.logger.info(f"Fetching OHLCV data from {start_date} to {end_date} ...")
        dfs = []
        for q in quote_objects:
            df = q.history(start=start_date, end=end_date, interval=interval)
            df["symbol"] = q.symbol
            dfs.append(df)
        self.logger.info(f"Fetched OHLCV data ({len(dfs)} rows).")

        dfs =  pd.concat(dfs, ignore_index=True)
        cols_to_select = ["symbol", "time", "open", "high", "low", "close", "volume"]
        return dfs[cols_to_select].sort_values(by=["time"], ascending=[True]).reset_index(drop=True)


    # FETCH FINANCIAL RATIOS
    def fetch_ratio_df(self, start_year: int, end_year: int = None, period: str = "year") -> pd.DataFrame:
        finance_objects = self._fetch_symbol_objects(object_type="finance")

        if end_year is None:
            end_year = datetime.today().year

        self.logger.info(f"Fetching financial ratios by {period} from {start_year} to {end_year} ...")
        dfs = []
        for f in finance_objects:
            df = f.ratio(period=period, lang='en')
            df["symbol"] = f.symbol
            dfs.append(df)
        self.logger.info(f"Fetched financial ratios ({len(dfs)} rows).")
        
        dfs = pd.concat(dfs, ignore_index=True)

        if isinstance(dfs.columns, pd.MultiIndex):
            dfs.columns = [(col[1] if col[1] != "" else col[0]).strip() for col in dfs.columns]

        if end_year is not None:
            dfs = dfs[(dfs["yearReport"] >= start_year) & (dfs["yearReport"] <= end_year)].reset_index(drop=True)
        else:
            dfs = dfs[dfs["yearReport"] >= start_year].reset_index(drop=True)

        return dfs.sort_values(by=['yearReport'], ascending=[True]).reset_index(drop=True)