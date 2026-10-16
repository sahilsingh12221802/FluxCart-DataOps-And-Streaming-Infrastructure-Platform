FROM apache/spark:4.0.0

USER root

WORKDIR /opt/fluxcart

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY schemas/ ./schemas/
COPY jobs/ ./jobs/

RUN mkdir -p /opt/fluxcart/data /opt/fluxcart/checkpoints \
    && chown -R spark:spark /opt/fluxcart

USER spark

CMD ["python3", "/opt/fluxcart/app/main.py"]
