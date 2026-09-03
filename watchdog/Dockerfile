FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY src /app/src
COPY watchdog/watchdog.py /app/watchdog.py
COPY watchdog/entrypoint.sh /app/entrypoint.sh

RUN useradd --system --uid 10001 topus \
    && mkdir -p /data /run/topus-secrets \
    && chown -R topus:topus /data /app /run/topus-secrets \
    && chmod 0755 /app/entrypoint.sh

ENV PYTHONUNBUFFERED=1
EXPOSE 8090
ENTRYPOINT ["/app/entrypoint.sh"]
