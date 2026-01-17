import os
import json
import datetime
import time
import requests
import gspread
import jpholiday
import yfinance as yf
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials

# --- 設定・定数 ---
SECRETS_JSON_ENV = 'GCP_CREDENTIALS_JSON'
MARKET_CAP_THRESHOLD = 500 * 100_000_000  # 小型株判定: 500億円
INVENTORY_RATIO_THRESHOLD = 0.3           # 棚卸資産警告: 30%
FINANCE_KEYWORDS = ["銀行業", "保険業", "証券", "商品先物", "金融業"]

# ヘッダー定義 (B列以降の書き込み用)
HEADER = [
    '銘柄名',           # B
    '業種',             # C
    'NC比率',           # D
    'NC比率1倍超',      # E
    '金融除外フラグ',    # F
    '時価総額(億)',     # G
    '小型株フラグ',      # H
    '棚卸資産警告',      # I
    '棚卸資産比率',      # J
    '流動資産',         # K
    '投資有価証券',      # L
    '負債合計',         # M
    'ネットキャッシュ',  # N
    '棚卸資産'          # O
]

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
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        # 1. 銘柄名取得 (Titleタグから抽出が一番確実)
        # 例: "トヨタ自動車(株)【7203】：株価・株式情報 - Yahoo!ファイナンス"
        title_text = soup.title.string if soup.title else ""
        name = "取得失敗"
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

        return name.strip(), industry.strip()

    except Exception as e:
        print(f"Scraping Error {ticker_code}: {e}")
        return "取得失敗", "取得失敗"

def get_financial_data(ticker_code):
    """yfinanceから財務データを取得して計算"""
    yf_ticker = yf.Ticker(f"{ticker_code}.T")
    
    try:
        # info取得 (時価総額)
        info = yf_ticker.info
        market_cap = info.get('marketCap', None)

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
                'status': 'DATA_MISSING'
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
            'inv_ratio': inv_ratio
        }

    except Exception as e:
        print(f"yfinance Error {ticker_code}: {e}")
        return {'status': 'ERROR'}

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

    # 出力用バッファ (ヘッダーはスプシに書くロジックで対応、ここはデータ行)
    output_rows = []

    print(f"Start processing {len(tickers)} tickers...")

    for code_raw in tickers:
        code_str = str(code_raw).strip()
        if not code_str:
            output_rows.append([""] * len(HEADER)) # 空行維持
            continue
        
        print(f"Processing: {code_str}")
        
        # A. Yahoo JP スクレイピング
        name_jp, industry_jp = get_yahoo_jp_info(code_str)
        time.sleep(1) # スクレイピング負荷軽減

        # B. yfinance データ取得
        fin_data = get_financial_data(code_str)
        time.sleep(1) # API負荷軽減

        # C. データ整形
        row_data = [""] * len(HEADER)
        
        # 基本情報
        row_data[0] = name_jp # B: 銘柄名
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
            row_data[0] = name_jp if name_jp != "取得失敗" else "データ取得失敗"
            row_data[2] = "ERROR/MISSING"

        output_rows.append(row_data)

    # 5. 書き込み
    # B1以降にヘッダー書き込み
    # B2以降にデータ書き込み
    # range指定: B1:O1 (Header), B2:O{len+1} (Data)
    
    # ヘッダー更新
    header_range = f"B1:{chr(65 + len(HEADER))}1" # O列まで
    worksheet.update(range_name=header_range, values=[HEADER])

    # データ更新
    if output_rows:
        end_row = 1 + len(output_rows)
        data_range = f"B2:{chr(65 + len(HEADER))}{end_row}"
        worksheet.update(range_name=data_range, values=output_rows)
    
    print("Done.")

if __name__ == "__main__":
    main()
