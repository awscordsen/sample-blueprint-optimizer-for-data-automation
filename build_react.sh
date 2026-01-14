#!/bin/bash

# Build React app for production
set -e  # Exit on any error

echo "Building React frontend for production..."

REACT_DIR="$(dirname "$0")/src/frontend/react"
if ! cd "$REACT_DIR"; then
    echo "Error: Failed to change to React directory: $REACT_DIR" >&2
    exit 1
fi

# Install dependencies if node_modules doesn't exist
if [ ! -d "node_modules" ]; then
    echo "Installing React dependencies..."
    if ! npm install; then
        echo "Error: npm install failed" >&2
        exit 1
    fi
fi

# Build the React app
echo "Building React app..."
if ! npm run build; then
    echo "Error: npm run build failed" >&2
    exit 1
fi

echo "React build completed. Files are in src/frontend/react/dist/"
echo "Start the FastAPI server to serve the React app at http://localhost:8000"
