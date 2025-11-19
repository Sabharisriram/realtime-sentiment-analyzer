#!/bin/bash

# Wait for Ollama to be ready and pull the model
echo "Waiting for Ollama service to be ready..."
until curl -s http://ollama:11434/api/tags > /dev/null 2>&1; do
    echo "Ollama not ready yet, waiting..."
    sleep 5
done

echo "Ollama is ready. Pulling llama3 model..."
curl -X POST http://ollama:11434/api/pull -d '{"name": "llama3"}' &
PULL_PID=$!

# Wait a bit for the pull to start, then start the consumer
sleep 10

echo "Starting sentiment consumer..."
python consumer.py