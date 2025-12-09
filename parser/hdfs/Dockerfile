FROM python:3.11-slim

WORKDIR /app/parser/hdfs

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt


COPY parser/cache_manager.py /app/parser/
COPY parser/log_processor.py /app/parser/

COPY parser/hdfs/parse_hdfs.py /app/parser/hdfs/
COPY parser/hdfs/hdfs_processor.py /app/parser/hdfs/
COPY parser/hdfs/vectorize_hdfs.py /app/parser/hdfs/
COPY parser/hdfs/analyze_hdfs.py /app/parser/hdfs/

COPY parser/drain.ini ./app/parser/

RUN mkdir -p /data/hdfs/raw /data/hdfs/parsed /data/hdfs/vectorized /data/hdfs/analysis /data/hdfs/state

# run the parser script (file name corrected)
CMD ["python", "parse_hdfs.py"]

