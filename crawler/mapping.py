# from elasticsearch import Elasticsearch, exceptions as es_exceptions

# # Kết nối đến Elasticsearch
# es = Elasticsearch(['https://big-data.kb.asia-southeast1.gcp.elastic-cloud.com:9243'])

# # Định nghĩa mapping cho index
# # mapping = {
# #     "mappings": {
# #         "properties": {
# #             "time": {"type": "date"},
# #             "open": {"type": "integer"},
# #             "high": {"type": "integer"},
# #             "low": {"type": "integer"},
# #             "close": {"type": "integer"},
# #             "volume": {"type": "integer"},
# #             "ticker": {"type": "keyword"}
# #         }
# #     }
# # }

# # mapping = {
# #     "mappings": {
# #         "properties": {
# #             "borough": {"type": "keyword"},
# #             "cuisine": {"type": "keyword"},
# #             "grades": {
# #                 "type": "nested",
# #                 "properties": {
# #                     "grade": {"type": "keyword"},
# #                     "score": {"type": "integer"}
# #                 }
# #             },
# #             "name": {"type": "text"},
# #             "restaurant_id": {"type": "keyword"}
# #         }
# #     }
# # }

# # mapping = {
# #         "mappings": {
# #             "properties": {
# #                 "ticker": {"type": "keyword"},
# #                 "total_volume": {"type": "long"},
# #             }
# #         }
# #     }

# mapping = {
#   "mappings": {
#     "properties": {
#       "ticker": { "type": "keyword" },
#       "companyType": { "type": "keyword" },
#       "total_volume": { "type": "long" },
#       "max_high": { "type": "double" },
#       "min_low": { "type": "double" },
#       "first_time": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
#       "first_open_value": { "type": "double" },
#       "end_time": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
#       "end_open_value": { "type": "double" },
#       "price_range_percent": { "type": "double" },
#       "growth": { "type": "double" }
#     }
#   }
# }


# # Tạo index với mapping được định nghĩa và kiểm tra kết quả
# index_name = "hao_hao"
# try:
#     result = es.indices.create(index=index_name, body=mapping)
#     if result["acknowledged"]:
#         print(f"Mapping cho index '{index_name}' đã được tạo thành công.")
#     else:
#         print(f"Không thể tạo mapping cho index '{index_name}'.")
# except es_exceptions.RequestError as e:
#     print(f"Lỗi khi tạo mapping cho index '{index_name}': {e}")
# except Exception as e:
#     print(f"Có lỗi xảy ra: {e}")
import requests
import json

# Định nghĩa mapping
mapping = {
    "mappings": {
        "properties": {
            "ticker": {"type": "keyword"},
            "companyType": {"type": "keyword"},
            "total_volume": {"type": "long"},
            "max_high": {"type": "double"},
            "min_low": {"type": "double"},
            "first_time": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "first_open_value": {"type": "double"},
            "end_time": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "end_open_value": {"type": "double"},
            "price_range_percent": {"type": "double"},
            "growth": {"type": "double"}
        }
    }
}

mapping_json = json.dumps(mapping)
index_name = "hao_hao"
elasticsearch_endpoint = "https://big-data.es.asia-southeast1.gcp.elastic-cloud.com"

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Apikey UbKgiVWIGT6vFJPu6sYmYR7A'  # Thay thế yourApiKeyHere bằng API Key của bạn
}

try:
    response = requests.put(f"{elasticsearch_endpoint}/{index_name}", headers=headers, data=mapping_json)
    if response.status_code == 200:
        print(f"Mapping cho index '{index_name}' đã được gửi thành công.")
    else:
        print(f"Lỗi khi gửi mapping: {response.text}")
except requests.RequestException as e:
    print(f"Lỗi kết nối: {e}")
