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
import pandas as pd # タイムスタンプ計算用に明示インポート

# --- 設定・定数 ---
SECRETS_JSON_ENV = 'GCP_CREDENTIALS_JSON'
MARKET_CAP_THRESHOLD = 500 * 100_000_000
INVENTORY_RATIO_THRESHOLD = 0.3
FINANCE_KEYWORDS = ["銀行業", "保険業", "証券", "商品先物", "金融業"]

# ==========================================
# ★ 設定: B/C列のスクレイピング・入力切替
# True : スクレイピングを行い、B列(銘柄名)・C列(業種)から更新 (低速)
# False: スクレイピングを行わず、D列(NC比率)から更新 (高速・30分短縮)
# ==========================================
UPDATE_BC_WITH_SCRAPING = False

# 東証33業種リスト
TSE_SECTORS = [
    "水産・農林業", "鉱業", "建設業", "食料品", "繊維製品", "パルプ・紙", "化学",
    "医薬品", "石油・石炭製品", "ゴム製品", "ガラス・土石製品", "鉄鋼", "非鉄金属",
    "金属製品", "機械", "電気機器", "輸送用機器", "精密機器", "その他製品",
    "電気・ガス業", "陸運業", "海運業", "空運業", "倉庫・運輸関連業", "情報・通信業",
    "卸売業", "小売業", "銀行業", "証券、商品先物取引業", "保険業",
    "その他金融業", "不動産業", "サービス業"
]

# ヘッダー定義 (並び順を変更: 安全性→収益性→カタリスト→フィルタ→在庫→生データ)
HEADER = [
    '銘柄名', '業種', 
    'NC比率', '厳格NC比率', 'NC1倍超', '厳格NC1倍超',  # ①安全性
    '実質PER', '配当利回り', '配当性向',               # ②収益性・③カタリスト (追加)
    '金融除外フラグ', '時価総額(億)', '小型株フラグ', # ④フィルタ
    '棚卸資産警告', '棚卸資産比率',                   # ⑤在庫リスク
    '流動資産(億)', '投資有価証券(億)', '負債合計(億)', 
    'ネットキャッシュ(億)', '厳格NC(億)', '棚卸資産(億)',
    '営業利益(億)'                                  # ⑥生データ (追加)
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
    # 修正: totalを5回に増加、backoff_factorを2に増加（待機時間をより長く確保）
    retry = Retry(
        total=5, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=frozenset(["GET"]),
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
    """Yahoo!ファイナンス(JP)から銘柄名と業種を取得"""
    url = f"https://finance.yahoo.co.jp/quote/{ticker_code}.T"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    
    # 修正: スクレイピング部分に強力なリトライループを追加
    # エラーが出ても、最大3回まで「長い休憩」を挟んで再挑戦する
    for i in range(3):
        try:
            # 修正: 待機時間を 0.05-0.5秒 → 1.0-2.0秒 に大幅延長
            time.sleep(random.uniform(1.0, 2.0))
            
            res = _HTTP_SESSION.get(url, headers=headers, timeout=10)
            res.raise_for_status()
            
            res.encoding = res.apparent_encoding
            html = res.text
            soup = BeautifulSoup(html, 'html.parser')

            # 1. 銘柄名取得
            title_text = soup.title.string if soup.title else ""
            name = None
            if "【" in title_text:
                name = title_text.split("【")[0]
            elif title_text:
                name = title_text.split("：")[0]

            # 2. 業種取得
            industry = "取得失敗"
            for candidate in TSE_SECTORS:
                if candidate in html:
                    industry = candidate
                    break

            return name.strip() if name else None, industry

        except Exception as e:
            # 最後のリトライでも失敗した場合のみエラーを出力
            if i == 2:
                print(f"Scraping Error for {ticker_code}: {e}")
                return None, "取得失敗"
            else:
                # 修正: エラー時は 5〜10秒 待機してサーバー負荷が下がるのを待つ
                time.sleep(random.uniform(5.0, 10.0))

    return None, "取得失敗"

def get_financial_data(ticker_code, jp_name_failed=False):
    """yfinanceから財務データ(BS/PL/Div)を取得して計算"""
    target_ticker = f"{ticker_code}.T"
    yf_ticker = yf.Ticker(target_ticker)
    
    fallback_name = None

    try:
        # 404エラーチェック
        try:
            _ = yf_ticker.fast_info.currency
        except Exception as e:
             return {'status': 'DATA_MISSING'}

        # 【対策1】時価総額・現在株価のリトライ取得
        # アクセス拒否(429)対策として、失敗時にWaitを入れて再試行する
        market_cap = None
        current_price = None
        
        # 修正: 最大リトライ回数を8回に増加（より粘り強く）
        for i in range(8): 
            try:
                # バージョン揺れ対応
                if hasattr(yf_ticker, "fast_info"):
                    market_cap = yf_ticker.fast_info.market_cap
                    current_price = yf_ticker.fast_info.last_price
                
                if market_cap is not None:
                    break
            except:
                pass
            # 修正: 待機時間を延長 (2.0秒 + 回数分)
            time.sleep(2.0 + i) 

        # 英語名フォールバック
        if jp_name_failed:
            try:
                fallback_name = yf_ticker.info.get('longName')
            except:
                pass

        # --- 1. BS取得 (安全性指標) ---
        # 【対策3】年次データがない場合、四半期データへの切り替え
        bs = None
        try:
            bs = yf_ticker.balance_sheet
            if bs is None or bs.empty:
                bs = yf_ticker.quarterly_balance_sheet
        except:
            pass

        if bs is None or bs.empty:
            return None

        latest_date_bs = bs.columns[0]
        latest_bs = bs[latest_date_bs]

        # 【対策2】データ項目名の「ゆらぎ」対応（柔軟性）
        # インデックスをすべて小文字化・空白除去してマッピングを作成
        bs_idx_map = {str(k).strip().lower(): k for k in latest_bs.index}

        def get_val_bs(key_list):
            for k in key_list:
                # 検索キーも正規化して探す
                search_key = str(k).strip().lower()
                if search_key in bs_idx_map:
                    real_key = bs_idx_map[search_key]
                    val = latest_bs[real_key]
                    if hasattr(val, "item"): 
                        if val != val: return None # NaN check
                        return float(val)
                    return float(val)
            return None

        total_assets_curr = get_val_bs(['Total Current Assets', 'Current Assets'])
        total_liab = get_val_bs(['Total Liabilities Net Minority Interest', 'Total Liabilities', 'Total Liab'])
        inventory = get_val_bs(['Inventory']) or 0.0
        
        inv_securities = 0.0
        found_inv = get_val_bs(['Investments', 'Other Short Term Investments', 'Long Term Investments', 'Other Investments'])
        if found_inv is not None:
            inv_securities = found_inv
        
        # --- 2. PL取得 (実質PER用) ---
        operating_income = 0.0
        basic_eps = 0.0
        
        fin = yf_ticker.financials
        if fin is None or fin.empty:
            # PLも四半期へフォールバック
            fin = yf_ticker.quarterly_financials

        if fin is not None and not fin.empty:
            latest_date_pl = fin.columns[0]
            latest_fin = fin[latest_date_pl]
            
            # PL用のゆらぎ対応マップ
            pl_idx_map = {str(k).strip().lower(): k for k in latest_fin.index}

            def get_val_pl(key_list):
                for k in key_list:
                    search_key = str(k).strip().lower()
                    if search_key in pl_idx_map:
                        real_key = pl_idx_map[search_key]
                        val = latest_fin[real_key]
                        if hasattr(val, "item"):
                             if val != val: return 0.0
                             return float(val)
                        return float(val)
                return 0.0
            
            # 営業利益
            operating_income = get_val_pl(['Operating Income', 'Operating Profit'])
            # EPS (配当性向用)
            basic_eps = get_val_pl(['Basic EPS'])

        # --- 3. 配当取得 (カタリスト用) ---
        # 過去1年間の配当合計を計算
        annual_dividend = 0.0
        try:
            divs = yf_ticker.dividends
            if not divs.empty:
                # タイムゾーン考慮: 今から1年前
                one_year_ago = pd.Timestamp.now(tz=divs.index.tz) - pd.DateOffset(years=1)
                recent_divs = divs[divs.index >= one_year_ago]
                annual_dividend = recent_divs.sum()
        except:
            pass

        # --- 計算処理 ---
        
        # 欠損チェック (BS必須)
        if market_cap is None or total_assets_curr is None or total_liab is None:
            return {
                'market_cap': market_cap,
                'status': 'DATA_MISSING',
                'fallback_name': fallback_name
            }

        # 安全性: NetCash
        net_cash = total_assets_curr + (inv_securities * 0.7) - total_liab
        # 安全性: 厳格NetCash (棚卸除外)
        net_cash_strict = (total_assets_curr - inventory) + (inv_securities * 0.7) - total_liab

        # 比率計算
        nc_ratio = 0
        nc_ratio_strict = 0
        if market_cap:
            nc_ratio = net_cash / market_cap
            nc_ratio_strict = net_cash_strict / market_cap
        
        inv_ratio = (inventory / total_assets_curr) if total_assets_curr else 0

        # 収益性: 実質PER (CN-PER) = (時価総額 - 厳格NC) / (営業利益 * 0.65)
        # ※営業利益が赤字、または0の場合は計算不可(None)とする
        cn_per = None
        op_after_tax = operating_income * 0.65
        if op_after_tax > 0 and market_cap:
            # 分子がマイナス(現金の方が多い)なら、CN-PERはマイナスになる(正しい挙動)
            cn_per = (market_cap - net_cash_strict) / op_after_tax
        
        # カタリスト: 配当利回り & 性向
        div_yield = 0.0
        payout_ratio = 0.0
        
        if current_price and current_price > 0:
            div_yield = annual_dividend / current_price
            
        if basic_eps > 0:
            payout_ratio = annual_dividend / basic_eps

        return {
            'status': 'OK',
            'market_cap': market_cap,
            'total_current_assets': total_assets_curr,
            'total_liabilities': total_liab,
            'inventory': inventory,
            'investment_securities': inv_securities,
            'net_cash': net_cash,
            'net_cash_strict': net_cash_strict,
            'nc_ratio': nc_ratio,
            'nc_ratio_strict': nc_ratio_strict,
            'inv_ratio': inv_ratio,
            'fallback_name': fallback_name,
            # 追加項目
            'cn_per': cn_per,
            'div_yield': div_yield,
            'payout_ratio': payout_ratio,
            'operating_income': operating_income
        }

    except Exception as e:
        print(f"yfinance Error {ticker_code}: {e}")
        return {'status': 'ERROR'}

def process_ticker_wrapper(code_raw):
    # 【対策4】待機時間（スリープ）の配置戦略
    # 修正: 設定がONの場合のみ、待機とスクレイピングを実行
    
    name_jp = None
    industry_jp = "-"
    
    if UPDATE_BC_WITH_SCRAPING:
        # 待機時間を 2.0〜4.0秒 に拡大
        time.sleep(random.uniform(2.0, 4.0))
        
        # 文字列変換と ".0" の除去
        code_str = str(code_raw).strip()
        if code_str.endswith(".0"):
            code_str = code_str[:-2]

        if not code_str:
            return [""] * len(HEADER)
        
        # A. Yahoo JP スクレイピング
        name_jp, industry_jp = get_yahoo_jp_info(code_str)
    else:
        # OFFの場合はスクレイピング関連をスキップ
        code_str = str(code_raw).strip()
        if code_str.endswith(".0"):
            code_str = code_str[:-2]
        if not code_str:
            # OFF時も要素数は合わせる必要があるが、戻り値で調整する
             return [""] * (len(HEADER) - 2)

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
        is_nc_strict_over_1 = fin_data['nc_ratio_strict'] >= 1.0
        
        # 単位変換用 (億円)
        to_oku = 100_000_000

        # 安全性
        row_data[2] = round(fin_data['nc_ratio'], 2)
        row_data[3] = round(fin_data['nc_ratio_strict'], 2)
        row_data[4] = is_nc_over_1
        row_data[5] = is_nc_strict_over_1
        
        # 収益性・カタリスト (追加)
        cn_per_val = fin_data['cn_per']
        row_data[6] = round(cn_per_val, 1) if cn_per_val is not None else "-"
        
        # 修正: 文字列フォーマットを廃止し、数値をそのまま出力
        row_data[7] = fin_data['div_yield']
        row_data[8] = fin_data['payout_ratio']

        # フィルタ
        row_data[9] = is_exclude_fin
        row_data[10] = round(fin_data['market_cap'] / to_oku, 1)
        row_data[11] = is_small
        
        # 在庫
        row_data[12] = is_inv_red
        # 修正: 文字列フォーマットを廃止し、数値をそのまま出力
        row_data[13] = fin_data['inv_ratio']
        
        # 財務数値 (億円)
        row_data[14] = round(fin_data['total_current_assets'] / to_oku, 1)
        row_data[15] = round(fin_data['investment_securities'] / to_oku, 1)
        row_data[16] = round(fin_data['total_liabilities'] / to_oku, 1)
        row_data[17] = round(fin_data['net_cash'] / to_oku, 1)
        row_data[18] = round(fin_data['net_cash_strict'] / to_oku, 1)
        row_data[19] = round(fin_data['inventory'] / to_oku, 1)
        row_data[20] = round(fin_data['operating_income'] / to_oku, 1) # 追加
    
    else:
        row_data[0] = final_name if final_name != "取得失敗" else "データ取得失敗"
        row_data[2] = "ERROR/MISSING"

    # 設定に応じて戻り値を変更
    if UPDATE_BC_WITH_SCRAPING:
        return row_data
    else:
        # B, C列(index 0, 1)を除外して、NC比率以降を返す
        return row_data[2:]

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

    # 修正: ヘッダー書き込み位置の調整
    if UPDATE_BC_WITH_SCRAPING:
        header_range = f"B1:{chr(65 + len(HEADER))}1"
        worksheet.update(range_name=header_range, values=[HEADER])
    else:
        # D列('NC比率')以降のヘッダーのみ更新
        # HEADER[2]は'NC比率'。B, Cをスキップするので D1から開始。
        # A(1), B(2), C(3), D(4) -> len(HEADER)-2 列分
        # HEADER[2:] を書き込む
        partial_header = HEADER[2:]
        # D列は chr(68)
        header_range = f"D1:{chr(68 + len(partial_header) - 1)}1"
        worksheet.update(range_name=header_range, values=[partial_header])

    print(f"Start processing {len(tickers)} tickers... (Scraping: {UPDATE_BC_WITH_SCRAPING})")

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
            
            # 修正: データ書き込み位置の調整
            if UPDATE_BC_WITH_SCRAPING:
                # B列から全て
                data_range = f"B{start_row}:{chr(65 + len(HEADER))}{end_row}"
            else:
                # D列から、NC比率以降のみ
                # batch_rowsの中身はすでに process_ticker_wrapper で短くなっている
                data_range = f"D{start_row}:{chr(68 + len(batch_rows[0]) - 1)}{end_row}"
            
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
