# Dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONUNBUFFERED=1

# Streamlit-порт
EXPOSE 8501

# Читаем настройки из переменных окружения
ENV STREAMLIT_SERVER_HEADLESS=true

CMD ["sh", "-c", "\
  streamlit run app/app.py \
    --server.port=${PORT:-8501} \
    --server.address=${STREAMLIT_SERVER_ADDRESS:-0.0.0.0} \
    --server.enableCORS=${STREAMLIT_ENABLE_CORS:-false} \
    --server.enableXsrfProtection=${STREAMLIT_ENABLE_XSRF_PROTECTION:-false} \
    --server.maxUploadSize=${STREAMLIT_MAX_UPLOAD_SIZE:-200} \
"]
