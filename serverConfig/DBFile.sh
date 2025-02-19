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
# Loop through each table and check if it exists
for TABLE_NAME in "${TABLES[@]}"; do
    if [ -n "$TABLE_NAME" ]; then  # Ensure TABLE_NAME is not empty
        TABLE_EXISTS=$(PGPASSWORD=$DB_PASSWORD psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -tAc "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='$TABLE_NAME');")
        echo $TABLE_EXISTS
        if [ "$TABLE_EXISTS" = "t" ]; then
            echo "Table $TABLE_NAME exists. Running specific commands..."
            # Add your commands here for each table
        else
            # Run specific commands based on table name
            if [ "$TABLE_NAME" = "all_db" ]; then
                echo "Unify all the databases"
                # python $PARENT_DIR/src/Commands/UnifyCSV.py
                echo "seeding Unified to the database"
                # python $PARENT_DIR/src/Commands/dynamic_table.py $PARENT_DIR/Data/output/unified_output.csv  all_db --primary 'verification_id'
            elif [ "$TABLE_NAME" = "region_country" ]; then
                echo "Extract country from data"
                # python $PARENT_DIR/src/Commands/CountryRegionManager.py
            elif [ "$TABLE_NAME" = "sense_config" ]; then
                echo "Database general settings ..."
                # python $PARENT_DIR/src/Commands/GeneralConfig.py
            fi
        fi
    fi
done


# #!/bin/bash

# # Get the absolute path of the script
# SCRIPT_PATH=$(realpath "$0")

# # Determine the script's directory
# SCRIPT_DIR=$(dirname "$SCRIPT_PATH")

# # Move up two directories
# PARENT_DIR=$(realpath "$SCRIPT_DIR/../")

# # Load environment variables from .env file
# export $(grep -v '^#' .env | xargs)

# # List of tables to check
# TABLES=("all_db" "region_country" "sense_config")

# # Directory where output files are stored
# CHECK_DIRECTORY="$PARENT_DIR/Data/output"
# echo "Checking directory: $CHECK_DIRECTORY"

# # Files to check
# FILES_TO_CHECK=("unified_output.csv")

# # Function to check if a file exists
# check_file() {
#     local file_path="$CHECK_DIRECTORY/$1"
#     if [ -f "$file_path" ]; then
#         echo "✅ File '$1' exists in directory '$CHECK_DIRECTORY'."
#     else
#         echo "❌ File '$1' does not exist in directory '$CHECK_DIRECTORY'."
#         echo "Pooling data from all sources..."
        
#         # Ensure Python script exists before executing
#         if [ -f "$PARENT_DIR/src/Commands/dataPool.py" ]; then
#             python3 "$PARENT_DIR/src/Commands/dataPool.py"
#         else
#             echo "❌ Error: Missing dataPool.py in $PARENT_DIR/src/Commands/"
#             exit 1
#         fi
#     fi
# }

# # Loop through file checks
# for FILE_NAME in "${FILES_TO_CHECK[@]}"; do
#     check_file "$FILE_NAME"
# done

# # Check tables using Python instead of `psql`
# python3 "$PARENT_DIR/serverConfig/check_tables.py" "${TABLES[@]}"
