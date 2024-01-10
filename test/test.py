import json
from vnstock import *
from datetime import datetime, timedelta
import time

# stock_array = ["ACB","BCM","BID","BVH","CTG","FPT","GAS","GVR","DHB","HPG","MBB","MSN",
#                "MWG","PLX","POW","SAB","SHB","SSB","TCB","TPB","VCB","VHM","VIB","VIC","VJC","VNM","VPB","VRE"]

def get_stock_data(symbol):
    today = datetime.now()
    start_date_this_week = today - timedelta(days=today.weekday())
    start_date_last_week = start_date_this_week - timedelta(days=0)
    end_date_last_week = start_date_last_week + timedelta(days=0)
    start_date_last_week_str = start_date_last_week.strftime('%Y-%m-%d')
    end_date_last_week_str = end_date_last_week.strftime('%Y-%m-%d')
    
    df = stock_historical_data(symbol=symbol,
                               start_date=start_date_last_week_str,
                               end_date=end_date_last_week_str,
                               resolution='1',
                               type='stock',
                               beautify=True)
    # Chuyển dữ liệu thành JSON với thêm thông tin về mã chứng khoán
    df['time'] = pd.to_datetime(df['time'])
    # df['time'] = df['time'].dt.strftime('%Y-%m-%d')
    df['time'] = df['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(x) else None)
    json_data = df.to_json(orient='records')
    return json_data

def get_stock_data_intraday(symbol): 
    df = stock_intraday_data(
            symbol=symbol, page_size=1, investor_segment=True
        )
    df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S')
    # df['time'] = df['time'].dt.strftime('%H:%M:%S') #1

    df['total_minutes'] = df['time'].dt.hour * 60 + df['time'].dt.minute - 9*60 -15
    df['time'] = df['time'].dt.strftime('%H:%M:%S') #1

    # Loại bỏ các dòng trùng lặp dựa trên cột 'total_minutes'
    df = df.drop_duplicates(subset=['total_minutes'])

    json_data = df.to_json(date_format='iso', orient='records')
    return json_data

if __name__ == "__main__":
    # all_stock_data = []
    # while True:
    #     for symbol in stock_array:
    #         stock_data = get_stock_data(symbol)
    #         all_stock_data.append(stock_data)
    #         print(stock_data)  # Lấy dữ liệu cho mã chứng khoán hiện tại
    #         # time.sleep(2)  # Chờ 2 giây trước khi lấy dữ liệu cho mã chứng khoán tiếp theo
    #     break

    # stock_data = get_stock_data_intraday("HPG")
    stock_data  = get_stock_data_intraday("HPG")
    print(stock_data)
    # Lưu dữ liệu vào một tệp JSON
    with open('stock_data.json', 'w') as file:
        json.dump(stock_data, file, indent=4)
        # file.write(stock_data)
