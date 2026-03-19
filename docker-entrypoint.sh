#!/bin/bash

# Docker entrypoint script for fakesnow
set -e

# Default values
FAKESNOW_PORT=${FAKESNOW_PORT:-8080}
FAKESNOW_HOST=${FAKESNOW_HOST:-0.0.0.0}
FAKESNOW_DB_PATH=${FAKESNOW_DB_PATH:-/app/databases}

# Create database directory if it doesn't exist
mkdir -p "$FAKESNOW_DB_PATH"
mkdir -p /app/logs

# Function to run fakesnow server
run_server() {
    echo "Starting fakesnow server..."
    echo "  Host: $FAKESNOW_HOST"
    echo "  Port: $FAKESNOW_PORT"
    echo "  Database path: $FAKESNOW_DB_PATH"
    echo "  Connection details:"
    echo "    - User: fake"
    echo "    - Password: snow"
    echo "    - Account: fakesnow"
    echo "    - Host: localhost (or container IP)"
    echo "    - Port: $FAKESNOW_PORT"
    echo "    - Protocol: http"
    echo ""
    
    # Set up environment variables for the server
    export FAKESNOW_DB_PATH
    
    # Start the server using uvicorn directly for better control
    exec python -m uvicorn fakesnow.server:app \
        --host "$FAKESNOW_HOST" \
        --port "$FAKESNOW_PORT" \
        --log-level info \
        --access-log \
        --use-colors
}

# Function to run fakesnow CLI with custom command
run_cli() {
    echo "Running fakesnow CLI with arguments: $*"
    exec python -m fakesnow "$@"
}

# Function to run a custom Python script
run_python() {
    echo "Running Python script: $*"
    exec python "$@"
}

# Function to show help
show_help() {
    cat << EOF
fakesnow Docker Container

Usage:
  docker run fakesnow [COMMAND] [ARGS...]

Commands:
  server                    Start the fakesnow HTTP server (default)
  cli [args]               Run fakesnow CLI with arguments
  python [script]          Run Python script
  bash                     Start bash shell
  help                     Show this help message

Environment Variables:
  FAKESNOW_PORT            Port to run server on (default: 8080)
  FAKESNOW_HOST            Host to bind server to (default: 0.0.0.0)
  FAKESNOW_DB_PATH         Path to store databases (default: /app/databases)

Examples:
  # Run server on custom port
  docker run -e FAKESNOW_PORT=9000 -p 9000:9000 fakesnow

  # Run with persistent database
  docker run -v \$(pwd)/data:/app/databases fakesnow

  # Run CLI command
  docker run fakesnow cli --help

  # Run Python script with fakesnow
  docker run fakesnow python myscript.py
EOF
}

# Parse command
case "${1:-server}" in
    server)
        run_server
        ;;
    cli)
        shift
        run_cli "$@"
        ;;
    python)
        shift
        run_python "$@"
        ;;
    bash|sh|shell)
        echo "Starting bash shell..."
        exec /bin/bash
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac