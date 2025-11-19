FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install PyTorch CPU version first with increased timeout
RUN pip install --no-cache-dir --default-timeout=300 \
    torch==2.0.1 \
    --index-url https://download.pytorch.org/whl/cpu

# Create a requirements-no-torch.txt without torch line
RUN grep -v "^torch==" requirements.txt > requirements-no-torch.txt

# Install other requirements with timeout
RUN pip install --no-cache-dir --default-timeout=300 -r requirements-no-torch.txt

# Copy application files
COPY consumer.py .
COPY start.sh .
RUN chmod +x start.sh

CMD ["./start.sh"]