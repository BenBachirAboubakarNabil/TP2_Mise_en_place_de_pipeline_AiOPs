FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY notebooks/ /app/notebooks/

RUN mkdir -p /data/hdfs/raw /data/hdfs/parsed /data/hdfs/vectorized \
             /data/hdfs/analysis /data/hdfs/state /data/hdfs/models \
             /data/hdfs/processed


EXPOSE 8888

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--allow-root", "--NotebookApp.token=''", "--notebook-dir=/app/notebooks"]