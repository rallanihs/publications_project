# Use Python base image
FROM python:3.9-slim

# Install system dependencies for Playwright and Selenium/Chrome
RUN apt-get update && \
    apt-get install -y curl wget gnupg unzip gnupg2 lsb-release ca-certificates \
                       libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
                       libxcomposite1 libxrandr2 libxdamage1 libxfixes3 libx11-xcb1 \
                       libxkbcommon0 libxcb1 libdbus-1-3 libdrm2 libgbm1 libasound2 \
                       libpangocairo-1.0-0 libpango-1.0-0 libgtk-3-0 libxshmfence1 libepoxy0 \
                       fonts-liberation libappindicator3-1 xdg-utils \
                       unzip xvfb && \
    rm -rf /var/lib/apt/lists/*

# Pin Chrome + Chromedriver to same version (140 here)
ARG CHROME_VERSION=140.0.7269.0

# Install Chromium
RUN wget -O /tmp/chrome.zip https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chrome-linux64.zip && \
    unzip /tmp/chrome.zip -d /opt/ && \
    rm /tmp/chrome.zip && \
    ln -s /opt/chrome-linux64/chrome /usr/bin/chromium

# Install Chromedriver
RUN wget -O /tmp/chromedriver.zip https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chromedriver-linux64.zip && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ && \
    rm /tmp/chromedriver.zip && \
    ln -s /usr/local/bin/chromedriver-linux64/chromedriver /usr/bin/chromedriver

# Set working directory
WORKDIR /app

# Copy Python dependencies and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Functions Framework
RUN pip install functions-framework

# Install Playwright and browsers
RUN pip install playwright && playwright install

ARG CACHEBUST=${GITHUB_SHA}

# Copy the rest of the code
COPY . .

# Expose Cloud Run port
EXPOSE 8080

# Run the Functions Framework targeting your @http function
CMD ["bash", "-c", "Xvfb :99 -screen 0 1280x1024x24 & export DISPLAY=:99 && functions-framework --target=download_pdfs_http --port=8080"]
