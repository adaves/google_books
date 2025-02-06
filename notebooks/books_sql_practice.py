import sqlite3
import os
from pathlib import Path
import pandas as pd

# Connect to the database, or create it if it doesn't exist
connection = sqlite3.connect(":memory:")
# Create a cursor object
# cursor object allows us to send SQL statements to a SQLite db
cursor = connection.cursor()

def load_data() -> pd.DataFrame:
    """
    This function loads the data from a csv into a Pandas dataframe.

    Returns:
    pd.DataFrame: A dataframe containing the data from the csv file.

    Raises:
    FileNotFoundError: If the csv file does not exist.
    pd.errors.EmptyDataError: If the csv file is empty.
    pr.errors.ParseError: If there is a parsing error while reading the csv file.
    """

    # Construct a dynamic file path to the script
    script_dir = Path(__file__).resolve().parent

    # Construct a dynamic file path to the dataset
    data_path = script_dir / 'combined_books.csv'

    # Load the csv into a DataFrame with error handling
    try:            
        df = pd.read_csv(data_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {data_path}")
    except pd.errors.EmptyDataError:
        raise pd.errors.EmptyDataError(f"No data: {data_path}")
    except pd.errors.ParserError:
        raise pd.errors.ParserError(f"Parsing error: {data_path}")
    except Exception as e:
        raise Exception(f"An error occurred: {e}")
    
    df['PublishedDate'] = pd.to_datetime(df["PublishedDate"])
    df['PublishedDate_Year'] = df['PublishedDate'].dt.year
    return df

def create_table():
# Create a table
    books_db = cursor.execute('''CREATE TABLE IF NOT EXISTS books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    title TEXT, 
                    author TEXT, 
                    year INTEGER
                    )''')
    connection.commit()

    return books_db
    
    

def insert_data_into_db(df, database):

    try:
        # Convert dataframe to list of tuples for fast insertion
        records = df[['Title', 'Authors', 'PublishedDate_Year']].values.tolist()

        insert_query = '''
            INSERT INTO books (title, author, year)
            VALUES (?, ?, ?)
        '''
        cursor.executemany(insert_query, records)
        connection.commit()

        print(f'Successfully inserted {len(records)} records into {database}')

    except Exception as e:
        print(f'Error inserting data: {str(e)}')
        connection.rollback()

def get_data_from_db():

    # cursor.execute("SELECT * FROM books")
    existing_data = cursor.execute("SELECT * FROM books").fetchall()
    print("Existing IDs:", existing_data)

def clear_db():
    pass


if __name__ == "__main__":

    books_db = create_table()

    df = load_data()
    insert_data_into_db(df, books_db)
    get_data_from_db()
