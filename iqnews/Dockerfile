FROM apache/airflow:3.0.0

COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt

USER root

# Instal Chrome
RUN apt-get update && apt-get install -y \
    wget \
    gnupg2 \
    ca-certificates \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list' \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instal Python packages yang diperlukan
RUN pip install --no-cache-dir \
    selenium \
    beautifulsoup4 \
    webdriver-manager \
    apache-airflow-providers-mongo