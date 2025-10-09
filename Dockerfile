# Dockerfile personalizado para Airflow con Firefox (versión optimizada)
FROM apache/airflow:3.0.6

USER root

# Instalar solo lo esencial: Firefox y curl (para webdriver-manager)
RUN apt-get update && apt-get install -y \
    firefox-esr \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instalar dependencias Python adicionales
RUN pip install --no-cache-dir \
    requests \
    pandas \
    selenium \
    webdriver-manager