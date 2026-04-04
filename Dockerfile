FROM python:3.11-slim

# HF Spaces run as non-root
RUN useradd -m -u 1000 agent
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=agent:agent . .
USER agent

EXPOSE 7860

# Healthcheck so HF Space shows green
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import requests; requests.post('http://localhost:7860/reset', json={'task':'easy'}, timeout=5)"

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7860", "--workers", "1"]
