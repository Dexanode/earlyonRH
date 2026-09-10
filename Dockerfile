FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY events.py listener.py ./
RUN useradd --uid 10001 --create-home collector && mkdir /app/data && chown collector /app/data
USER collector
CMD ["python", "listener.py", "run"]
