FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PIPELINE_CONFIG=configs/pipeline.yaml

CMD ["sh", "-c", "python -m src.main --config ${PIPELINE_CONFIG}"]
