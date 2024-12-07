#!/bin/bash

# Default values
INTERVAL=300  # 5 minutes
NUM_FILES=1
NUM_RECORDS=1000
FILE_TYPES=("transactions" "customers")
OUTPUT_DIR="./data/incoming"
API_URL="http://localhost:8000"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --interval)
            INTERVAL="$2"
            shift 2
            ;;
        --num-files)
            NUM_FILES="$2"
            shift 2
            ;;
        --num-records)
            NUM_RECORDS="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --api-url)
            API_URL="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Start file watcher in background
echo "Starting file watcher..."
python3 file_watcher.py \
    --watch-dir "$OUTPUT_DIR" \
    --api-url "$API_URL" \
    --patterns .csv &

WATCHER_PID=$!

# Trap Ctrl+C to cleanup
cleanup() {
    echo "Stopping simulation..."
    kill $WATCHER_PID
    exit 0
}
trap cleanup SIGINT

echo "Starting file generation simulation..."
echo "Output directory: $OUTPUT_DIR"
echo "Interval: $INTERVAL seconds"
echo "Files per interval: $NUM_FILES"
echo "Records per file: $NUM_RECORDS"

while true; do
    for ((i=1; i<=$NUM_FILES; i++)); do
        # Randomly select file type
        FILE_TYPE=${FILE_TYPES[$RANDOM % ${#FILE_TYPES[@]}]}
        
        echo "Generating $FILE_TYPE file..."
        python3 file_generator.py \
            --output-dir "$OUTPUT_DIR" \
            --file-type "$FILE_TYPE" \
            --num-records "$NUM_RECORDS"
        
        # Random delay between files (1-5 seconds)
        sleep $((RANDOM % 5 + 1))
    done
    
    echo "Waiting $INTERVAL seconds before next generation..."
    sleep "$INTERVAL"
done
