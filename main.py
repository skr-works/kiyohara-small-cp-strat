import os
import json
import datetime
import time
import requests
import gspread
import jpholiday
import yfinance as yf
import random
import re
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import traceback

# --- 設定・定数 ---
SECRETS_JSON_ENV = 'GCP_CREDENTIALS_JSON'
MARKET_CAP_THRESHOLD = 500 * 100_000_000
INVENTORY_RATIO_THRESHOLD = 0.3
FINANCE_KEYWORDS = ["銀行業", "保険業", "証券", "商品先物", "金融業"]

# 東証33業種リスト (参照コードより追加)
TSE_SECTORS = [
    "水産・農林業", "鉱業", "建設業", "食料品", "繊維製品", "パルプ・紙", "化学",
    "医薬品", "石油・石炭製品", "ゴム製品", "ガラス・土石製品", "鉄鋼", "非鉄金属",
    "金属製品", "機械", "電気機器", "輸送用機器", "精密機器", "その他製品",
    "電気・ガス業", "陸運業", "海運業", "空運業", "倉庫・運輸関連業", "情報・通信業",
    "卸売業", "小売業", "銀行業", "証券、商品先物取引業", "保険業",
    "その他金融業", "不動産業", "サービス業"
]

# ヘッダー定義 (単位変更に伴い名称修正)
HEADER = [
    '銘柄名', '業種', 'NC比率', 'NC比率1倍超', '金融除外フラグ', 
    '時価総額(億)', '小型株フラグ', '棚卸資産警告', '棚卸資産比率', 
    '流動資産(億)', '投資有価証券(億)', '負債合計(億)', 'ネットキャッシュ(億)', '棚卸資産(億)'
]

# User-Agentリスト
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# Session作成
def create_session():
    session = requests.Session()
    retry = Retry(
        total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=frozenset(["GET"]),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session

_HTTP_SESSION = create_session()

def is_market_closed():
    """休場日判定"""
    today = datetime.date.today()
    if today.weekday() >= 5:
        print(f"Today is weekend ({today.strftime('%A')}). Exiting.")
        return True
    if jpholiday.is_holiday(today):
        print(f"Today is holiday ({jpholiday.holiday_name(today)}). Exiting.")
        return True
    if (today.month == 12 and today.day == 31) or (today.month == 1 and today.day <= 3):
        print("Today is New Year holidays. Exiting.")
        return True
    return False

def get_yahoo_jp_info(ticker_code):
    """Yahoo!ファイナンス(JP)から銘柄名と業種を取得 (リスト照合方式に変更)"""
    url = f"https://finance.yahoo.co.jp/quote/{ticker_code}.T"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    
    try:
        time.sleep(random.uniform(0.05, 0.5))
        res = _HTTP_SESSION.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        
        # HTMLテキスト全体を取得
        res.encoding = res.apparent_encoding
        html = res.text
        soup = BeautifulSoup(html, 'html.parser')

        # 1. 銘柄名取得 (Titleタグから抽出)
        title_text = soup.title.string if soup.title else ""
        name = None
        if "【" in title_text:
            name = title_text.split("【")[0]
        elif title_text:
            name = title_text.split("：")[0]

        # 2. 業種取得 (修正: HTML全体から東証33業種リストに含まれるものを探す)
        industry = "取得失敗"
        for candidate in TSE_SECTORS:
            # HTML内に候補文字列が含まれているかチェック
            if candidate in html:
                industry = candidate
                break

        return name.strip() if name else None, industry

    except Exception as e:
        print(f"Scraping Error for {ticker_code}: {e}")
        return None, "取得失敗"

def get_financial_data(ticker_code, jp_name_failed=False):
    """yfinanceから財務データを取得して計算"""
    target_ticker = f"{ticker_code}.T"
    yf_ticker = yf.Ticker(target_ticker)
    
    fallback_name = None

    try:
        # 404エラーチェック
        try:
             _ = yf_ticker.fast_info.currency
        except Exception as e:
             return {'status': 'DATA_MISSING'}

        # 時価総額
        market_cap = None
        try:
            market_cap = yf_ticker.fast_info.market_cap
        except:
            pass
            
        # 英語名フォールバック
        if jp_name_failed:
            try:
                fallback_name = yf_ticker.info.get('longName')
            except:
                pass

        # BS取得
        bs = yf_ticker.balance_sheet
        if bs.empty:
            return None

        latest_date = bs.columns[0]
        latest_bs = bs[latest_date]

        def get_val(key_list):
            for k in key_list:
                if k in latest_bs.index:
                    val = latest_bs[k]
                    if hasattr(val, "item"): 
                        if val != val: return None
                    return float(val)
            return None

        # 'Total Current Assets' がない場合 'Current Assets' を探す
        total_assets_curr = get_val(['Total Current Assets', 'Current Assets'])
        
        total_liab = get_val(['Total Liabilities Net Minority Interest', 'Total Liabilities'])
        inventory = get_val(['Inventory']) or 0.0
        
        inv_securities = 0.0
        found_inv = get_val(['Investments', 'Other Short Term Investments', 'Long Term Investments', 'Other Investments'])
        if found_inv is not None:
            inv_securities = found_inv
        
        # 欠損チェック
        if market_cap is None or total_assets_curr is None or total_liab is None:
            return {
                'market_cap': market_cap,
                'status': 'DATA_MISSING',
                'fallback_name': fallback_name
            }

        # 計算
        net_cash = total_assets_curr + (inv_securities * 0.7) - total_liab
        nc_ratio = net_cash / market_cap if market_cap else 0
        inv_ratio = (inventory / total_assets_curr) if total_assets_curr else 0

        return {
            'status': 'OK',
            'market_cap': market_cap,
            'total_current_assets': total_assets_curr,
            'total_liabilities': total_liab,
            'inventory': inventory,
            'investment_securities': inv_securities,
            'net_cash': net_cash,
            'nc_ratio': nc_ratio,
            'inv_ratio': inv_ratio,
            'fallback_name': fallback_name
        }

    except Exception as e:
        print(f"yfinance Error {ticker_code}: {e}")
        return {'status': 'ERROR'}

def process_ticker_wrapper(code_raw):
    # 文字列変換と ".0" の除去
    code_str = str(code_raw).strip()
    if code_str.endswith(".0"):
        code_str = code_str[:-2]

    if not code_str:
        return [""] * len(HEADER)
    
    # A. Yahoo JP スクレイピング (リスト照合版)
    name_jp, industry_jp = get_yahoo_jp_info(code_str)
    
    # B. yfinance データ取得
    fin_data = get_financial_data(code_str, jp_name_failed=(name_jp is None))
    
    # C. データ整形
    row_data = [""] * len(HEADER)
    
    final_name = name_jp
    if not final_name and fin_data and fin_data.get('fallback_name'):
        final_name = fin_data['fallback_name']
    if not final_name:
        final_name = "取得失敗"

    row_data[0] = final_name
    row_data[1] = industry_jp

    if fin_data and fin_data['status'] == 'OK':
        is_exclude_fin = any(k in industry_jp for k in FINANCE_KEYWORDS)
        is_small = fin_data['market_cap'] <= MARKET_CAP_THRESHOLD
        is_inv_red = fin_data['inv_ratio'] >= INVENTORY_RATIO_THRESHOLD
        is_nc_over_1 = fin_data['nc_ratio'] >= 1.0
        
        # 単位変換用 (億円)
        to_oku = 100_000_000

        row_data[2] = round(fin_data['nc_ratio'], 2)
        row_data[3] = is_nc_over_1
        row_data[4] = is_exclude_fin
        row_data[5] = round(fin_data['market_cap'] / to_oku, 1) # 時価総額(億)
        row_data[6] = is_small
        row_data[7] = is_inv_red
        row_data[8] = f"{fin_data['inv_ratio']:.2%}"
        
        # 修正: 以下の財務数値を億円単位に変換
        row_data[9] = round(fin_data['total_current_assets'] / to_oku, 1)  # 流動資産(億)
        row_data[10] = round(fin_data['investment_securities'] / to_oku, 1) # 投資有価証券(億)
        row_data[11] = round(fin_data['total_liabilities'] / to_oku, 1)     # 負債合計(億)
        row_data[12] = round(fin_data['net_cash'] / to_oku, 1)              # ネットキャッシュ(億)
        row_data[13] = round(fin_data['inventory'] / to_oku, 1)             # 棚卸資産(億)
    
    else:
        row_data[0] = final_name if final_name != "取得失敗" else "データ取得失敗"
        row_data[2] = "ERROR/MISSING"

    return row_data

def main():
    force_run = os.environ.get("FORCE_RUN") == "true"

    if force_run:
        print("FORCE_RUN is enabled. Skipping holiday check.")
    elif is_market_closed():
        return

    secrets_json = os.environ.get(SECRETS_JSON_ENV)
    if not secrets_json:
        raise ValueError(f"Environment variable {SECRETS_JSON_ENV} not found.")

    creds_dict = json.loads(secrets_json)
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    sheet_url = creds_dict.get('spreadsheet_url')
    sheet_name = creds_dict.get('sheet_name')
    spreadsheet = client.open_by_url(sheet_url)
    worksheet = spreadsheet.worksheet(sheet_name)

    col_a = worksheet.col_values(1)
    tickers = col_a[1:] 
    if not tickers:
        print("No tickers found in column A.")
        return

    header_range = f"B1:{chr(65 + len(HEADER))}1"
    worksheet.update(range_name=header_range, values=[HEADER])

    print(f"Start processing {len(tickers)} tickers...")

    BATCH_SIZE = 50
    total_tickers = len(tickers)
    current_index = 0

    while current_index < total_tickers:
        end_index = min(current_index + BATCH_SIZE, total_tickers)
        batch_tickers = tickers[current_index:end_index]
        
        print(f"Processing batch: {current_index + 1} - {end_index} / {total_tickers}")
        
        batch_rows = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = executor.map(process_ticker_wrapper, batch_tickers)
            batch_rows = list(results)

        if batch_rows:
            start_row = current_index + 2
            end_row = start_row + len(batch_rows) - 1
            data_range = f"B{start_row}:{chr(65 + len(HEADER))}{end_row}"
            
            try:
                worksheet.update(range_name=data_range, values=batch_rows)
                time.sleep(2)
            except Exception as e:
                print(f"Sheet write error at batch index {current_index}: {e}")

        current_index += BATCH_SIZE
        time.sleep(2)
    
    print("Done.")

if __name__ == "__main__":
    main()
