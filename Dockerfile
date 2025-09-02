# Use Python base image
FROM python:3.9-slim

# Install system dependencies for Playwright and Selenium/Chrome
RUN apt-get update && \
    apt-get install -y curl wget gnupg libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
                       libxcomposite1 libxrandr2 libxdamage1 libxfixes3 libx11-xcb1 \
                       libxkbcommon0 libxcb1 libdbus-1-3 libdrm2 libgbm1 libasound2 \
                       libpangocairo-1.0-0 libpango-1.0-0 libgtk-3-0 libxshmfence1 libepoxy0 \
                       fonts-liberation libappindicator3-1 xdg-utils \
                       unzip xvfb \
                       && rm -rf /var/lib/apt/lists/*

# Install Chrome (stable) for Selenium
RUN apt-get update && apt-get install -y wget gnupg2 lsb-release ca-certificates && \
    wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor > /usr/share/keyrings/google-linux-signing-key.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-key.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy Python dependencies and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Functions Framework
RUN pip install functions-framework

# Install Playwright and browsers
RUN pip install playwright && playwright install

# Copy the rest of the code
COPY . .

# Expose Cloud Run port
EXPOSE 8080

# Run the Functions Framework targeting your @http function
CMD ["bash", "-c", "Xvfb :99 -screen 0 1280x1024x24 & export DISPLAY=:99 && functions-framework --target=download_pdfs_http --port=8080"]

