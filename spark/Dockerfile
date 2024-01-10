FROM bde2020/spark-submit:3.3.0-hadoop3.3

LABEL maintainer="Your Name <your.email@example.com>"
# Create /app directory in the container
RUN mkdir /app
RUN rm /submit.sh
COPY ./submit.sh /

RUN echo "Build number: 1"
# Copy ./app/template.sh from local to /
COPY ./template.sh /

# Copy all files from ./app in local to /app in container
COPY ./app.py ./hadoop_to_elastic.py ./requirements.txt /app/

# Set the working directory to /app
WORKDIR /app
# RUN /usr/bin/python3.7 -m pip install --upgrade pip
# Optional: Install dependencies if needed
RUN pip3 install -r requirements.txt

# Specify the command to run when the container starts
CMD ["/bin/bash", "/template.sh"]
