# Sử dụng image Python cơ bản
FROM python:latest

# Cài đặt các package cần thiết
RUN pip install requests
RUN pip install pandas
RUN pip install plotly==5.3.1
RUN pip install retrying==1.3.3
RUN pip install confluent-kafka
RUN pip install vnstock
RUN pip install beautifulsoup4

# Copy file Python của bạn vào container
COPY ./crawler.py /app/crawler.py

# Chạy file Python khi container được khởi động
CMD ["bash", "-c", "python /app/crawler.py && tail -f /dev/null"]
