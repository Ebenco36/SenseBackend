import psycopg2
import sys
import os

# Get database URL from environment
DATABASE_URL = os.getenv("DATABASE_URL")

# Extract table names from shell script arguments
tables = sys.argv[1:]

def check_table_exists(table_name, cursor):
    """Check if a table exists in PostgreSQL."""
    cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name=%s);", (table_name,))
    return cursor.fetchone()[0]

def execute_task(table_name):
    """Run specific commands based on table existence."""
    parent_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), "../"))
    
    if table_name == "all_db":
        print("Unify all the databases...")
        os.system(f"python {parent_dir}/src/Commands/UnifyCSV.py")
        print("Seeding Unified to the database...")
        os.system(f"python {parent_dir}/src/Commands/dynamic_table.py {parent_dir}/Data/output/unified_output.csv all_db --primary 'verification_id'")

    elif table_name == "region_country":
        print("Extracting country from data...")
        os.system(f"python {parent_dir}/src/Commands/CountryRegionManager.py")

    elif table_name == "sense_config":
        print("Database general settings...")
        os.system(f"python {parent_dir}/src/Commands/GeneralConfig.py")

def main():
    if not DATABASE_URL:
        print("❌ Error: DATABASE_URL not set in environment variables.")
        sys.exit(1)

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        for table in tables:
            if check_table_exists(table, cursor):
                print(f"✅ Table {table} exists. Running specific commands...")
            else:
                print(f"❌ Table {table} does not exist. Running fallback operations...")
                execute_task(table)

        # Close connection
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
