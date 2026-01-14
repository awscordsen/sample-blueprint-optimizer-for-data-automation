#!/bin/bash

set -e  # Exit on any error

# Install dependencies
echo "Installing dependencies..."
if ! python3 -m pip install -r requirements.txt; then
    echo "Error: Failed to install dependencies" >&2
    exit 1
fi

# Create necessary directories if they don't exist
mkdir -p src/frontend/templates
mkdir -p src/frontend/static

# Run the FastAPI application
echo "Starting FastAPI server..."
if ! python3 -m uvicorn src.frontend.app:app --host 0.0.0.0 --port 8000 --reload; then
    echo "Error: Failed to start uvicorn server" >&2
    exit 1
fi
