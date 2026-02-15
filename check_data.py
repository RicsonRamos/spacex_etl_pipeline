import sqlite3
import pandas as pd
from pathlib import Path

def audit_database():
    """
    Prints a summary of the database, including the number of records
    in each table and the last update time for each table.
    Also prints the overall historical success rate of SpaceX launches.
    """
    db_path = Path("data/database/spacex_prod.db")
    if not db_path.exists():
        print("Error: Database not found.")
        return

    conn = sqlite3.connect(db_path)
    tables = ['launches', 'rockets', 'payloads', 'launchpads']
    
    print(f"{'Table':<15} | {'Records':<10} | {'Last Update'}")
    print("-" * 50)
    
    with conn:
        cur = conn.cursor()
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count, = cur.fetchone()
            cur.execute(f"SELECT MAX(rowid) FROM {table}")
            last_update, = cur.fetchone()
            print(f"{table:<15} | {count:<10} | {last_update[0]}")

    # Quick Insight: SpaceX Overall Success Rate
    print("-" * 50)
    cur.execute("SELECT AVG(success) * 100 FROM launches WHERE success IS NOT NULL")
    success_rate, = cur.fetchone()
    print(f"Historical Success Rate (Launches): {success_rate[0]:.2f}%")
    
    conn.close()

if __name__ == "__main__":
    audit_database()