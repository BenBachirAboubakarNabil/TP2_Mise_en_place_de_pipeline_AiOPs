FROM python:3.11-slim

WORKDIR /app


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


RUN pip install --no-cache-dir jupyter


COPY notebooks/ /app/notebooks/


RUN mkdir -p /data/openstack/raw \
             /data/openstack/parsed \
             /data/openstack/vectorized \
             /data/openstack/processed \
             /data/openstack/models \
             /data/openstack/analysis \
             /data/openstack/archive \
             /data/openstack/state

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''", "--notebook-dir=/app/notebooks"]