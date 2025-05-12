FROM apache/airflow:2.9.0-python3.9

# Switch to root to install system dependencies
USER root

# Update dan install dependensi system
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    build-essential \
    libgomp1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Alternative: Install packages directly
RUN pip install --no-cache-dir \
    pandas==1.5.3 \
    openpyxl==3.1.2 \
    pymongo==4.3.3 \
    yfinance==0.2.31 \
    requests==2.31.0 \
    lxml==5.0.0