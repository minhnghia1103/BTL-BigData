from confluent_kafka import Producer
import json
from vnstock import *
from datetime import datetime, timedelta
from time import sleep
import threading
import requests

# Hàm gửi dữ liệu JSON vào Kafka với mã chứng khoán
def produce_kafka_json(bootstrap_servers, topic_name, symbol, json_message):
    print("line 14",json_message)
    # Tạo producer chỉ định tên và địa chỉ Kafka broker
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    # Gửi 1 tin nhắn vào topic kafka đã chỉ định sau đó chuyển value và key thành dạng binary
    producer.produce(topic_name, value=json_message.encode('utf-8'), key=symbol.encode('utf-8'), callback=delivery_report)
    # Đợi cho đến khi tất cả tin nhắn được gửi đi
    producer.flush()

# Hàm callback cho việc gửi tin nhắn
def delivery_report(err, msg):
    """Callback được gọi khi tin nhắn được gửi thành công hoặc gặp lỗi."""
    if err is not None:
        print('Gửi tin nhắn thất bại: {}'.format(err))
    else:
        print('Tin nhắn được gửi thành công: {}'.format(msg.key().decode('utf-8')))


        
def get_stock_data(bitcoin_code):
    # Thêm mã Bitcoin vào địa chỉ URL của API
    api_url = f"http://json-server:3000/{bitcoin_code}"

    # Gửi yêu cầu GET đến API
    response = requests.get(api_url)

    # Kiểm tra xem yêu cầu có thành công hay không (status code 200 là thành công)
    if response.status_code == 200:
        # Lấy dữ liệu JSON từ phản hồi
        data = response.json()

        # Định dạng lại trường Date để chỉ chứa ngày tháng năm
        for entry in data:
            if 'Date' in entry:
                entry['Date'] = datetime.strptime(entry['Date'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d')
            else:
                print("Warning: 'Date' key not found in entry")

        # Trả về dữ liệu đã được xử lý
        return json.dumps(data)
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return None

def get_stock_day(day):
    # Thêm mã Bitcoin vào địa chỉ URL của API
    api_url = f"http://json-server:3000/Bitcoin{day}"

    # Gửi yêu cầu GET đến API
    response = requests.get(api_url)

    # Kiểm tra xem yêu cầu có thành công hay không (status code 200 là thành công)
    if response.status_code == 200:
        # Lấy dữ liệu JSON từ phản hồi
        data = response.json()

        # Định dạng lại trường Date để chỉ chứa ngày tháng năm
        for entry in data:
            if 'Date' in entry:
                entry['Date'] = datetime.strptime(entry['Date'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d')
            else:
                print("Warning: 'Date' key not found in entry")

        # Trả về dữ liệu đã được xử lý
        return json.dumps(data)
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return None

def jobCrawlCoinData(kafka_topic, bootstrap_servers):
    stock_array = ["Bitcoin","Ethereum","Tether","Cardano","Dogecoin"]
    # stock_array = ["Bitcoin"]

    while True:
        for symbol in stock_array:
            stock_data = get_stock_data(symbol)
            produce_kafka_json(bootstrap_servers, kafka_topic, symbol, stock_data)  # Gửi dữ liệu vào Kafka với mã chứng khoán
            time.sleep(10)  # Chờ 2 giây trước khi lấy dữ liệu cho mã chứng khoán tiếp theo
        break

def realtime(kafka_topic, bootstrap_servers):
    for i in range(1, 10):
        stock_data = get_stock_day(i)
        produce_kafka_json(bootstrap_servers, kafka_topic, "Bitcoin", stock_data) 
        time.sleep(5)

if __name__ == "__main__":
    bootstrap_servers = 'kafka:9092'  # Thay thế bằng địa chỉ Kafka broker của bạn
    kafka_topic_coin = 'coin'  
    kafka_topic_maketCap = 'realtime'

    t1 = threading.Thread(target=jobCrawlCoinData, args=(kafka_topic_coin, bootstrap_servers))
    t2 = threading.Thread(target=realtime, args=( kafka_topic_maketCap, bootstrap_servers))

    t1.start()
    t2.start()

    t1.join()
    t2.join()