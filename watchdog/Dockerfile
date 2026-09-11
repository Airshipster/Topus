FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY src /app/src
COPY tests /app/tests
COPY watchdog/watchdog.py /app/watchdog.py
COPY watchdog/watchdog.py /app/watchdog/watchdog.py
COPY watchdog/controller.py /app/controller.py
COPY watchdog/entrypoint.sh /app/entrypoint.sh
RUN python -m compileall -q /app/src /app/controller.py && python -m unittest discover -s /app/tests

RUN useradd --system --uid 10001 topus \
    && mkdir -p /data /run/topus-secrets \
    && chown -R topus:topus /data /app /run/topus-secrets \
    && sed -i 's/\r$//' /app/entrypoint.sh \
    && chmod 0755 /app/entrypoint.sh

ENV PYTHONUNBUFFERED=1
EXPOSE 8090
ENTRYPOINT ["/app/entrypoint.sh"]
