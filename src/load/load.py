class SpaceXLoader:
    def __init__(self, engine):
        self.engine = engine
    
    def load_tables(self, tables):

        for name, df in tables.items():
            df.to_sql(name, self.engine, index=False, if_exists='append')
        
        print(f"Loaded {name}: {len(df)} rows")