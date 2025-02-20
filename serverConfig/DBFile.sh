#!/bin/bash

# Get the absolute path of the script
SCRIPT_PATH=$(realpath "$0")

# Determine the script's directory
SCRIPT_DIR=$(dirname "$SCRIPT_PATH")

# Move up two directories
PARENT_DIR=$(realpath "$SCRIPT_DIR/../")

# Load environment variables from .env file
export $(grep -v '^#' .env | xargs)

# Extract database details from DATABASE_URL
DATABASE_URL=$(echo $DATABASE_URL)

# Use awk and sed to extract credentials
DB_USER=$(echo $DATABASE_URL | awk -F '[:/@]' '{print $4}')
DB_PASSWORD=$(echo $DATABASE_URL | awk -F '[:/@]' '{print $5}')
DB_HOST=$(echo $DATABASE_URL | awk -F '[:/@]' '{print $6}')
DB_PORT=$(echo $DATABASE_URL | awk -F '[:/@]' '{print $7}')
DB_NAME=$(echo $DATABASE_URL | awk -F '/' '{print $NF}')

TABLES=("all_db" "region_country" "sense_config") 
CHECK_DIRECTORY="$PARENT_DIR/Data/output"
echo $CHECK_DIRECTORY
FILES_TO_CHECK=("unified_output.csv")
# Function to check if a file exists
check_file() {
    local file_path="$CHECK_DIRECTORY/$1"
    if [ -f "$file_path" ]; then
        echo "✅ File '$1' exists in directory '$CHECK_DIRECTORY'."
    else
        echo "❌ File '$1' does not exist in directory '$CHECK_DIRECTORY'."
        echo "Pool data from all sources"
        python $PARENT_DIR/src/Commands/dataPool.py
    fi
}
for FILE_NAME in "${FILES_TO_CHECK[@]}"; do
    check_file "$FILE_NAME"
done
python "$PARENT_DIR/serverConfig/check_tables.py" "${TABLES[@]}"
