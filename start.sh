#!/bin/bash
# start.sh

echo "Starting Railway Gateway Server..."

# Set environment jika belum diset
export PORT=${PORT:-3000}
export NODE_ENV=${NODE_ENV:-production}

echo "PORT: $PORT"
echo "NODE_ENV: $NODE_ENV"

# Install dependencies jika node_modules tidak ada
if [ ! -d "node_modules" ]; then
  echo "Installing dependencies..."
  npm install --production
fi

# Start server
node server.js