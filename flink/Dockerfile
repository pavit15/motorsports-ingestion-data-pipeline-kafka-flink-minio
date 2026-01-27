FROM flink:1.17.2-scala_2.12-java11

# Install Python + pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

# Install Python dependencies
RUN pip3 install --no-cache-dir \
    kafka-python \
    pandas \
    pyarrow \
    s3fs

WORKDIR /opt/flink
