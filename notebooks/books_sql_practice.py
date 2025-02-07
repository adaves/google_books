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
    
    # Convert to datetime and then to string format
    df['PublishedDate'] = pd.to_datetime(df["PublishedDate"]).dt.strftime('%Y-%m-%d')
    df['PublishedDate_Year'] = pd.to_datetime(df["PublishedDate"]).dt.year
    return df

def create_table():
# Create a table
    books_db = cursor.execute('''CREATE TABLE IF NOT EXISTS books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    title TEXT NOT NULL, 
                    authors TEXT, 
                    publisher TEXT, 
                    published_date TEXT,
                    isbn TEXT,
                    page_count INTEGER,
                    categories TEXT,
                    average_rating FLOAT,
                    ratings_count INTEGER,
                    language TEXT,
                    title_word_count INTEGER,
                    year INTEGER
                    )''')
    connection.commit()

    return books_db
    
def insert_data_into_db(df, database):

    try:
        # Convert dataframe to list of tuples for fast insertion
        records = df[['Title', 'Authors', 'Publisher', 'PublishedDate', 'ISBN', 'PageCount', 'Categories', 'AverageRating', 'RatingsCount', 'Language', 'TitleWordCount', 'PublishedDate_Year']].values.tolist()

        insert_query = '''
            INSERT INTO books (title, authors, publisher, published_date, isbn, page_count, categories, average_rating, ratings_count, language, title_word_count, year)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cursor.executemany(insert_query, records)
        connection.commit()

        print(f'Successfully inserted {len(records)} records into {database}')

    except Exception as e:
        print(f'Error inserting data: {str(e)}')
        connection.rollback()

def higher_than_average_rating():
    query = """
        SELECT title, average_rating
        FROM books
        WHERE average_rating > (
            SELECT AVG(average_rating)
            FROM books
            WHERE average_rating IS NOT NULL
        )
        ORDER BY average_rating DESC
        LIMIT 5
    """
    return cursor.execute(query).fetchall()

def longer_than_avg_book_in_category():
    query = """
        SELECT
            title,
            categories,
            page_count
        FROM books AS b1
        WHERE page_count > (
            SELECT AVG(page_count)
            FROM books AS b2
            WHERE b2.categories = b1.categories AND page_count IS NOT NULL
        )
        ORDER BY page_count DESC
        LIMIT 5;
    """
    return cursor.execute(query).fetchall()

def authors_with_longer_than_avg_books():
    query = """
        SELECT
            DISTINCT(authors)
        FROM books
        WHERE page_count > (
            SELECT AVG(page_count)
            FROM books
        )
        AND authors IS NOT NULL
        LIMIT 5;
    """
    return cursor.execute(query).fetchall()

def top_rated_books_each_category():
    query = """
        SELECT
            DISTINCT(categories),
            title,
            MAX(average_rating) AS top_rated
        FROM books
        WHERE ratings_count > 5
        ORDER BY categories;
    """
    return cursor.execute(query).fetchall()

def publisher_data():
    query = """
        SELECT
            publisher,
            COUNT(*) AS num_books,
            AVG(page_count) AS average_page_count,
            MAX(average_rating) AS highest_rated
        FROM books
        GROUP BY publisher
        HAVING COUNT(*) >= 3
        ORDER BY num_books DESC;
    """
    return cursor.execute(query).fetchall()

def compare_books_published_same_year():
    query = """
        SELECT
            title,
            year,
            average_rating,
            AVG(average_rating) OVER(PARTITION BY year) AS year_avg_rating
        FROM books
        ORDER BY year DESC, average_rating DESC
        LIMIT 10;
    """
    return cursor.execute(query).fetchall()

def print_results(results, query_name):
    """
    print query results in a formatted way
    """
    print(f"\n{query_name} results:")
    print("-" * 50)
    for row in results:
        print(f"> {' | '.join(str(item) for item in row)}")
    print("-" * 50)


if __name__ == "__main__":

    books_db = create_table()

    df = load_data()
    insert_data_into_db(df, books_db)

    print_results(top_rated_books_each_category(), "Top rated books in each category")
    print_results(publisher_data(), "Publisher Data")
    print_results(compare_books_published_same_year(), "Compare books published in the same year")
