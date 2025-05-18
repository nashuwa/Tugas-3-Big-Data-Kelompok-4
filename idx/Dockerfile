FROM apache/airflow:3.0.0

USER root

# Install Chrome dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    libxss1 \
    libasound2 \
    libxtst6 \
    xvfb \
    libxi6 \
    libgbm1

# Install Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Install chromedriver for Chrome 115+
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}') \
    && CHROME_MAJOR_VERSION=$(echo $CHROME_VERSION | cut -d '.' -f 1) \
    && if [ "$CHROME_MAJOR_VERSION" -ge 115 ]; then \
        LATEST_VERSION=$(curl -s "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_$CHROME_MAJOR_VERSION") \
        && wget -q "https://storage.googleapis.com/chrome-for-testing-public/$LATEST_VERSION/linux64/chromedriver-linux64.zip" \
        && unzip chromedriver-linux64.zip \
        && mv chromedriver-linux64/chromedriver /usr/local/bin/ \
        && chmod +x /usr/local/bin/chromedriver \
        && rm -rf chromedriver-linux64 chromedriver-linux64.zip; \
    else \
        CHROMEDRIVER_VERSION=$(curl -s "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_MAJOR_VERSION") \
        && wget -q "https://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip" \
        && unzip chromedriver_linux64.zip -d /usr/local/bin \
        && chmod +x /usr/local/bin/chromedriver \
        && rm chromedriver_linux64.zip; \
    fi

# Create downloads directory
RUN mkdir -p /opt/airflow/downloads && chown -R airflow:root /opt/airflow/downloads

USER airflow
# Install Python dependencies
# Install Python dependencies
RUN pip install --no-cache-dir \
    selenium==4.19.0 \
    selenium-stealth==1.0.6 \
    pymongo==4.6.2 \
    pandas==2.2.2 \

    
