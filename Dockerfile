FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY events.py listener.py stream.py enricher.py alert_engine.py wallet_profiler.py market_normalizer.py topology.py capital_flow.py creator_graph.py social_enricher.py dashboard.py ./
COPY web ./web
RUN useradd --uid 10001 --create-home collector && mkdir /app/data && chown collector /app/data
USER collector
CMD ["python", "listener.py", "run"]
