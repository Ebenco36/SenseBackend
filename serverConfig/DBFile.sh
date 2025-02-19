#!/bin/bash

# Get the absolute path of the script
SCRIPT_PATH=$(realpath "$0")

# Determine the script's directory
SCRIPT_DIR=$(dirname "$SCRIPT_PATH")

# Move up two directories
PARENT_DIR=$(realpath "$SCRIPT_DIR/../")

echo "Database general settings ..."
python $PARENT_DIR/src/Commands/GeneralConfig.py

echo "Pool data from sources"
# python $PARENT_DIR/src/Commands/dataPool.py

# echo "seeding OVID to the database"
# python $PARENT_DIR/src/Commands/dynamic_table.py $PARENT_DIR/Data/OVIDNew/data.csv ovid_db --primary DOI --fallback 'Digital Object Identifier'
# echo "seeding L-OVE DB to the database"
# python $PARENT_DIR/src/Commands/dynamic_table.py $PARENT_DIR/Data/L-OVE/LOVE.csv  love_db --primary id
# echo "seeding Cochrane DB to the database"
# python $PARENT_DIR/src/Commands/dynamic_table.py $PARENT_DIR/Data/Cochrane/cochrane_combined_output.csv  cochrane_db --primary 'cdIdentifier'
# echo "seeding Medline to the database"
# python $PARENT_DIR/src/Commands/dynamic_table.py $PARENT_DIR/Data/MedlineData/medline_results.csv  medline_db --primary 'PMID'

echo "Unify all the databases"
python $PARENT_DIR/src/Commands/UnifyCSV.py

echo "seeding Unified to the database"
python $PARENT_DIR/src/Commands/dynamic_table.py $PARENT_DIR/Data/output/unified_output.csv  all_db --primary 'verification_id'

echo "Extract country from data"
python $PARENT_DIR/src/Commands/CountryRegionManager.py
