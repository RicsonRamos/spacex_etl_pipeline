import sys
import sqlite3
import pandas as pd
from pathlib import Path

# Add root path to sys.path for importing modules from any location
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

from src.config.config import settings

def run_audit():
    """
    Run a data audit on the SpaceX ETL SQLite database.

    This function performs the following checks:
    1. Counts the total records in key tables.
    2. Verifies the integrity of primary keys (checks for null values).
    3. Calculates business insights such as launch success rate and payload reuse.
    """
    print("\n" + "="*50)
    print("DATA AUDIT SYSTEM - SPACEX ETL")
    print("="*50)

    # Extract database path from configuration
    db_path = settings.DATABASE_URL.replace("sqlite:///", "")
    
    if not Path(db_path).exists():
        print(f"CRITICAL ERROR: Database not found at {db_path}")
        return

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    
    # List of tables to audit
    tables = ['rockets', 'launchpads', 'landpads', 'payloads', 'launches']
    
    print(f"{'TABLE':<12} | {'RECORDS':<10} | {'PK INTEGRITY'}")
    print("-" * 50)
    
    for table in tables:
        try:
            # Count total records
            count = pd.read_sql(f"SELECT COUNT(*) FROM {table}", conn).iloc[0, 0]
            
            # Determine primary key dynamically
            pk = "launch_id" if table == "launches" else f"{table.rstrip('s')}_id"
            
            # Check for null values in primary key
            null_pks = pd.read_sql(f"SELECT COUNT(*) FROM {table} WHERE {pk} IS NULL", conn).iloc[0, 0]
            
            pk_status = "OK" if null_pks == 0 else f"{null_pks} NULLS"
            print(f"{table:<12} | {count:<10} | {pk_status}")
            
        except Exception:
            print(f"{table:<12} | WARNING: Table not found or empty")

    print("-" * 50)
    print("BUSINESS INSIGHTS (DATA VALIDATION)")
    
    try:
        # Calculate global launch success rate
        query = "SELECT AVG(CAST(success AS FLOAT)) * 100 FROM launches WHERE success IS NOT NULL"
        success_rate = pd.read_sql(query, conn).iloc[0, 0]
        
        if success_rate is not None:
            print(f"Global Launch Success Rate: {success_rate:.2f}%")
        else:
            print("Global Launch Success Rate: N/A (insufficient data)")
            
        # Count reused payloads
        reused_count = pd.read_sql("SELECT COUNT(*) FROM payloads WHERE reused = 1", conn).iloc[0,0]
        print(f"Total Reused Payloads: {reused_count}")

    except Exception as e:
        print(f"Business metrics unavailable: {str(e)}")

    print("="*50 + "\n")
    conn.close()

if __name__ == "__main__":
    run_audit()
