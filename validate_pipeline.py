import pandas as pd
from sqlalchemy import create_engine, text
from src.config.config import settings

def validate():
    """
    Validate the database schema and perform a few sanity checks
    
    Reports on the number of records in each table, and the result of a JOIN query
    between the launches and rockets tables.
    """
    engine = create_engine(settings.DATABASE_URL)
    conn = engine.connect()
    
    # get the list of existing tables
    existing_tables = pd.read_sql_table("sqlite_master", conn, index_col="name", columns=["name"])[["name"]].tolist()
    
    print("\n" + "="*50)
    print("Validate Report")
    print("="*50)

    try:
        # print the number of records in each table
        for t in ['rockets', 'launches', 'ships']:
            if t in existing_tables:
                count = pd.read_sql_query(f"SELECT COUNT(*) as total FROM {t}", conn).iloc[0]['total']
                print(f"{t.upper()}: {count} registros.")
            else:
                print(f"{t.upper()}: Table not found.")
        
        # perform a JOIN query between the launches and rockets tables
        if 'rockets' in existing_tables and 'launches' in existing_tables:
            query = """
            SELECT 
                l.name as launch,
                r.name as rocket,
                l.date_utc
            FROM launches l
            JOIN rockets r ON l.rocket_id = r.rocket_id
            ORDER BY l.date_utc DESC
            LIMIT 5
            """
            print("\nRelationship integrity (JOIN):")
            df_join = pd.read_sql_query(query, conn)
            print(df_join if not df_join.empty else "no results.")
    
    except Exception as e:
        print(f"Validation failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    validate()