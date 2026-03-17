FROM python:3.11.8 AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HF_HOME=/opt/huggingface
WORKDIR /app


RUN python -m venv .venv
COPY requirements.txt ./
RUN .venv/bin/pip install -r requirements.txt
RUN .venv/bin/python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"
FROM python:3.11.8-slim
ENV HF_HOME=/opt/huggingface
WORKDIR /app
COPY --from=builder /app/.venv .venv/
COPY --from=builder /opt/huggingface /opt/huggingface
COPY . .
CMD ["/app/.venv/bin/fastapi", "run"]
