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

# --- 設定・定数 ---
SECRETS_JSON_ENV = 'GCP_CREDENTIALS_JSON'
MARKET_CAP_THRESHOLD = 500 * 100_000_000  # 小型株判定: 500億円
INVENTORY_RATIO_THRESHOLD = 0.3           # 棚卸資産警告: 30%
FINANCE_KEYWORDS = ["銀行業", "保険業", "証券", "商品先物", "金融業"]

# ヘッダー定義 (B列以降の書き込み用)
HEADER = [
    '銘柄名',            # B
    '業種',              # C
    'NC比率',            # D
    'NC比率1倍超',       # E
    '金融除外フラグ',    # F
    '時価総額(億)',      # G
    '小型株フラグ',      # H
    '棚卸資産警告',      # I
    '棚卸資産比率',      # J
    '流動資産',          # K
    '投資有価証券',      # L
    '負債合計',          # M
    'ネットキャッシュ',  # N
    '棚卸資産'           # O
]

# 追加: User-Agentリスト
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
]

# 追加: Session作成
def create_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session

_HTTP_SESSION = create_session()

def is_market_closed():
    """休場日判定（土日、祝日、年末年始）"""
    today = datetime.date.today()
    
    # 1. 土日判定 (月=0, ..., 日=6)
    if today.weekday() >= 5:
        print(f"Today is weekend ({today.strftime('%A')}). Exiting.")
        return True

    # 2. 祝日判定
    if jpholiday.is_holiday(today):
        print(f"Today is holiday ({jpholiday.holiday_name(today)}). Exiting.")
        return True

    # 3. 年末年始 (12/31 - 1/3)
    # 大納会(30日)まではやるが、31日～3日は休む仕様
    if (today.month == 12 and today.day == 31) or (today.month == 1 and today.day <= 3):
        print("Today is New Year holidays. Exiting.")
        return True

    return False

def get_yahoo_jp_info(ticker_code):
    """Yahoo!ファイナンス(JP)から銘柄名と業種をスクレイピング"""
    url = f"https://finance.yahoo.co.jp/quote/{ticker_code}.T"
    # 修正: User-Agentランダム化
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    
    try:
        # 修正: 待機時間短縮
        time.sleep(random.uniform(0.05, 0.9))
        
        # 修正: Session再利用
        res = _HTTP_SESSION.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        res.encoding = res.apparent_encoding # 文字化け対策
        soup = BeautifulSoup(res.text, 'html.parser')

        # 1. 銘柄名取得 (Titleタグから抽出が一番確実)
        # 例: "トヨタ自動車(株)【7203】：株価・株式情報 - Yahoo!ファイナンス"
        title_text = soup.title.string if soup.title else ""
        name = None # 修正: 失敗時はNoneで返す(フォールバック用)
        if "【" in title_text:
            name = title_text.split("【")[0]
        elif title_text:
            name = title_text.split("：")[0]

        # 2. 業種取得
        # YahooファイナンスのHTML構造に依存。
        # PC版では <div ...><span>業種</span><strong><a ...>電気機器</a></strong></div> の構造が多い
        industry = "取得失敗"
        # "業種" というテキストを持つ要素を探す
        target_el = soup.find(lambda tag: tag.name == "span" and "業種" in tag.text)
        if target_el:
            # その親の次の兄弟、あるいは親の中にあるstrong/aタグなどを探す
            # 構造: span(label) -> strong(value) -> a
            parent = target_el.parent
            industry_tag = parent.find("a")
            if industry_tag:
                industry = industry_tag.text.strip()
            else:
                # リンクがない場合もあるのでテキスト取得トライ
                industry = parent.text.replace("業種", "").strip()

        return name.strip() if name else None, industry.strip()

    except Exception as e:
        # 修正: ログから銘柄コード除外
        print(f"Scraping Error (masked): {e}")
        return None, "取得失敗"

def get_financial_data(ticker_code, jp_name_failed=False):
    """yfinanceから財務データを取得して計算"""
    yf_ticker = yf.Ticker(f"{ticker_code}.T")
    
    # 追加: 英語名フォールバック用変数
    fallback_name = None

    try:
        # 修正: 404エラー即時撤退 (fast_infoアクセスで確認)
        try:
             _ = yf_ticker.fast_info.currency
        except Exception as e:
             if "404" in str(e) or "Not Found" in str(e):
                 return {'status': 'DATA_MISSING'}

        # 修正: fast_infoへの移行 (時価総額)
        market_cap = None
        try:
            market_cap = yf_ticker.fast_info.market_cap
        except:
            pass
            
        # 追加: 英語名フォールバック (JPスクレイピング失敗時のみ info を取得)
        if jp_name_failed:
            try:
                fallback_name = yf_ticker.info.get('longName')
            except:
                pass

        # BS取得 (最新年度)
        bs = yf_ticker.balance_sheet
        if bs.empty:
            return None  # BSデータなし

        # 最新のカラム（日付）を取得
        latest_date = bs.columns[0]
        latest_bs = bs[latest_date]

        def get_val(key_list):
            for k in key_list:
                if k in latest_bs.index:
                    val = latest_bs[k]
                    # NaNチェック
                    if hasattr(val, "item"): # numpy type
                        if val != val: return None # NaN check
                    return float(val)
            return None

        # 項目取得
        total_assets_curr = get_val(['Total Current Assets'])
        total_liab = get_val(['Total Liabilities Net Minority Interest', 'Total Liabilities'])
        inventory = get_val(['Inventory']) or 0.0 # 棚卸はなければ0とみなす
        
        # 投資有価証券 (優先順位探索)
        inv_securities = 0.0
        found_inv = get_val(['Investments', 'Other Short Term Investments', 'Long Term Investments', 'Other Investments'])
        if found_inv is not None:
            inv_securities = found_inv
        
        # --- 計算 ---
        # 欠損チェック (InventoryとInvestment以外は必須)
        if market_cap is None or total_assets_curr is None or total_liab is None:
            return {
                'market_cap': market_cap,
                'status': 'DATA_MISSING',
                'fallback_name': fallback_name
            }

        # 清原式ネットキャッシュ = 流動資産 + (投資有価証券 * 0.7) - 負債
        net_cash = total_assets_curr + (inv_securities * 0.7) - total_liab
        
        # 比率
        nc_ratio = net_cash / market_cap if market_cap else 0
        
        # 棚卸資産比率
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
        # 修正: ログから銘柄コード除外
        print(f"yfinance Error (masked): {e}")
        return {'status': 'ERROR'}

# 追加: 並列処理用ラッパー関数
def process_ticker_wrapper(code_raw):
    code_str = str(code_raw).strip()
    if not code_str:
        return [""] * len(HEADER)
    
    # A. Yahoo JP スクレイピング
    name_jp, industry_jp = get_yahoo_jp_info(code_str)
    
    # B. yfinance データ取得 (名前取得失敗時はフラグを渡す)
    fin_data = get_financial_data(code_str, jp_name_failed=(name_jp is None))
    
    # C. データ整形
    row_data = [""] * len(HEADER)
    
    # 名前決定 (JP優先 -> 失敗時は英語名 -> それでもダメならエラー)
    final_name = name_jp
    if not final_name and fin_data and fin_data.get('fallback_name'):
        final_name = fin_data['fallback_name']
    if not final_name:
        final_name = "取得失敗"

    # 基本情報
    row_data[0] = final_name # B: 銘柄名
    row_data[1] = industry_jp # C: 業種

    if fin_data and fin_data['status'] == 'OK':
        # 金融除外フラグ
        is_exclude_fin = any(k in industry_jp for k in FINANCE_KEYWORDS)
        
        # 小型株フラグ
        is_small = fin_data['market_cap'] <= MARKET_CAP_THRESHOLD
        
        # 棚卸警告
        is_inv_red = fin_data['inv_ratio'] >= INVENTORY_RATIO_THRESHOLD
        
        # NC比率 1倍超
        is_nc_over_1 = fin_data['nc_ratio'] >= 1.0

        # 値詰め
        row_data[2] = round(fin_data['nc_ratio'], 2)          # D: NC比率
        row_data[3] = is_nc_over_1                            # E: NC比率1倍超
        row_data[4] = is_exclude_fin                          # F: 金融除外
        row_data[5] = round(fin_data['market_cap'] / 1e8, 1)  # G: 時価総額(億)
        row_data[6] = is_small                                # H: 小型株
        row_data[7] = is_inv_red                              # I: 棚卸警告
        row_data[8] = f"{fin_data['inv_ratio']:.2%}"          # J: 棚卸比率
        row_data[9] = fin_data['total_current_assets']        # K: 流動資産
        row_data[10] = fin_data['investment_securities']      # L: 投資有価証券
        row_data[11] = fin_data['total_liabilities']          # M: 負債合計
        row_data[12] = fin_data['net_cash']                   # N: ネットキャッシュ
        row_data[13] = fin_data['inventory']                  # O: 棚卸資産
    
    else:
        # 取得失敗時
        row_data[0] = final_name if final_name != "取得失敗" else "データ取得失敗"
        row_data[2] = "ERROR/MISSING"

    return row_data

def main():
    # ▼▼▼ 修正箇所開始 ▼▼▼
    # 環境変数 FORCE_RUN が "true" なら休日でも実行
    force_run = os.environ.get("FORCE_RUN") == "true"

    if force_run:
        print("FORCE_RUN is enabled. Skipping holiday check.")
    elif is_market_closed():
        # 強制実行でなく、かつ休日の場合は終了
        return
    # ▲▲▲ 修正箇所終了 ▲▲▲

    # 2. Secrets読み込み & GSpread認証
    secrets_json = os.environ.get(SECRETS_JSON_ENV)
    if not secrets_json:
        raise ValueError(f"Environment variable {SECRETS_JSON_ENV} not found.")

    creds_dict = json.loads(secrets_json)
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    # 3. スプシ取得
    sheet_url = creds_dict.get('spreadsheet_url')
    sheet_name = creds_dict.get('sheet_name')
    spreadsheet = client.open_by_url(sheet_url)
    worksheet = spreadsheet.worksheet(sheet_name)

    # 4. 銘柄コード読み込み (A列)
    # A1はヘッダー想定、A2以下を取得
    col_a = worksheet.col_values(1)
    tickers = col_a[1:] # Skip header
    if not tickers:
        print("No tickers found in column A.")
        return

    # ヘッダー更新 (初回)
    header_range = f"B1:{chr(65 + len(HEADER))}1" # O列まで
    worksheet.update(range_name=header_range, values=[HEADER])

    print(f"Start processing {len(tickers)} tickers...")

    # 修正: バッチ処理と並列化の導入
    BATCH_SIZE = 50
    total_tickers = len(tickers)
    current_index = 0

    while current_index < total_tickers:
        end_index = min(current_index + BATCH_SIZE, total_tickers)
        batch_tickers = tickers[current_index:end_index]
        
        print(f"Processing batch: {current_index + 1} - {end_index} / {total_tickers}")
        
        batch_rows = []
        # 並列処理実行
        with ThreadPoolExecutor(max_workers=4) as executor:
            # mapを使えば順序も保持される
            results = executor.map(process_ticker_wrapper, batch_tickers)
            batch_rows = list(results)

        # 5. 書き込み (バッチ単位)
        if batch_rows:
            # 書き込み開始行: ヘッダー(1) + 既処理数 + 1(1-based) => current_index + 2
            start_row = current_index + 2
            end_row = start_row + len(batch_rows) - 1
            data_range = f"B{start_row}:{chr(65 + len(HEADER))}{end_row}"
            
            try:
                worksheet.update(range_name=data_range, values=batch_rows)
                time.sleep(2) # API制限回避
            except Exception as e:
                print(f"Sheet write error at batch index {current_index}: {e}")

        current_index += BATCH_SIZE
        time.sleep(2) # バッチ間ウェイト
    
    print("Done.")

if __name__ == "__main__":
    main()
