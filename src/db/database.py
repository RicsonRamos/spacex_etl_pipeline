from sqlalchemy import create_engine, text


class DatabaseManager:

    def __init__(self, db_path='data/spacex.db'):
        self.engine = create_engine(f"sqlite:///{db_path}", echo=False)

    def get_engine(self):
        return self.engine
    def execute_query(self, query):
        
        with self.engine.connect() as conn:
            return conn.execute(text(query))