import pandas as pd
from sqlalchemy import create_engine, text
from src.config.config import settings

def validate_data():
    """
    Validates the data in the database by checking the integrity of the data 
    between the rockets and launches tables.

    Prints out the total number of launches, successes and success rate for each rocket.
    """
    try:
        # Creates connection using the environment variable string
        engine = create_engine(settings.DATABASE_URL)
        
        query = text("""
        SELECT 
            r.name AS Rocket,
            COUNT(l.launch_id) AS Total_Launches,
            SUM(CASE WHEN l.success = 1 THEN 1 ELSE 0 END) AS Successes,
            ROUND(AVG(CAST(l.success AS FLOAT)) * 100, 2) AS Success_Rate
        FROM rockets r
        INNER JOIN launches l ON r.rocket_id = l.rocket_id
        GROUP BY r.name
        ORDER BY Total_Launches DESC;
        """)
        
        # Executes the query and loads into a DataFrame using chunksize to reduce memory usage
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn, chunksize=1000)
            df_list = []
            for chunk in df:
                df_list.append(chunk)
            df = pd.concat(df_list, ignore_index=True)
        
        print("\n" + "="*50)
        print("DATA INTEGRITY REPORT (SpaceX)")
        print("="*50)
        
        if df.empty:
            print("Error: The database returned zero records.")
        else:
            print("Data successfully cross-referenced between Rockets and Launches:")
            print("-" * 50)
            print(df.to_string(index=False))
            
        print("="*50 + "\n")

    except Exception as e:
        print(f"Failed to validate database: {e}")

if __name__ == "__main__":
    validate_data()