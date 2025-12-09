FROM python:3.11-slim

WORKDIR /app/parser/openstack

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY parser/cache_manager.py /app/parser/
COPY parser/log_processor.py /app/parser/
COPY parser/drain.ini /app/parser/

COPY parser/openstack/parse_openstack.py /app/parser/openstack/
COPY parser/openstack/openstack_processor.py /app/parser/openstack/
COPY parser/openstack/vectorize_openstack.py /app/parser/openstack/

RUN mkdir -p /data/openstack/raw \
             /data/openstack/parsed \
             /data/openstack/vectorized \
             /data/openstack/processed \
             /data/openstack/models \
             /data/openstack/analysis \
             /data/openstack/archive \
             /data/openstack/state

CMD ["python", "parse_openstack.py"]
