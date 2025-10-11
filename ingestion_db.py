import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Create engine (SQLite is easiest for testing)
engine = create_engine('sqlite:///test_database.db')

def ingest_db(df, table_name, engine):
    '''this function will ingest the dataframe into database table'''
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Successfully inserted {len(df)} rows into table '{table_name}'")
        logging.info(f"Successfully inserted {len(df)} rows into table '{table_name}'")
    except Exception as e:
        print(f"Error inserting data for table '{table_name}': {e}")
        logging.error(f"Error inserting data for table '{table_name}': {e}")

def load_raw_data():
    start = time.time()  # Fixed: removed the argument
    
    # Your loop - properly indented inside the function
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            try:
                df = pd.read_csv(os.path.join('data', file))
                logging.info(f'Ingesting {file} in db')
                print(f"{file}: {df.shape}")
                ingest_db(df, file[:-4], engine)
            except Exception as e:
                print(f"Error processing {file}: {e}")
                logging.error(f"Error processing {file}: {e}")
    
    end = time.time()  # Moved inside the function
    total_time = (end - start) / 60
    logging.info('-----------------------Ingestion Complete------------------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')
    print(f'Total Time Taken: {total_time:.2f} minutes')

if __name__ == '__main__':
    load_raw_data()