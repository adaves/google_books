import requests
import random
import time
import os
import pandas as pd
from dotenv import load_dotenv
import logging

# Set up logging to a file
logging.basicConfig(
    filename="get_google_books_data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Load environment variables from .env file
load_dotenv()

# GET API KEY FROM .env file
API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")

if not API_KEY:
    raise ValueError("API Key not found. Please set it in your .env file.")


def get_random_query():
    """
    Generate a random search query from a list of popular topics.

    Returns:
        str: A randomly chosen topic from the list.
    """
    popular_topics = [
        "science",
        "history",
        "fiction",
        "technology",
        "biography",
        "art",
        "self-help",
        "adventure",
    ]
    return random.choice(popular_topics)


def fetch_books_data(target_count=10000):
    """
    Fetch book data from Google Books API with pagination.

    Args:
        target_count (int): The target number of books to fetch.

    Returns:
        list: A list of dictionaries containing book data.
    """
    books = []
    max_per_request = 40  # Maximum allowed by Google Books API

    while len(books) < target_count:
        query = get_random_query()
        start_index = 0

        while (
            start_index < 1000
        ):  # Google Books API limits startIndex to 1000 per query
            url = f"https://www.googleapis.com/books/v1/volumes?q={query}&startIndex={start_index}&maxResults={max_per_request}&key={API_KEY}"
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                if "items" not in data:
                    break  # No more items available for this query

                for item in data["items"]:
                    volume_info = item["volumeInfo"]
                    book = {
                        "Title": volume_info.get("title", "N/A"),
                        "Authors": ", ".join(volume_info.get("authors", ["N/A"])),
                        "Publisher": volume_info.get("publisher", "N/A"),
                        "PublishedDate": volume_info.get("publishedDate", "N/A"),
                        "ISBN": next(
                            (
                                identifier["identifier"]
                                for identifier in volume_info.get(
                                    "industryIdentifiers", []
                                )
                                if identifier["type"] == "ISBN_13"
                            ),
                            "N/A",
                        ),
                        "PageCount": volume_info.get("pageCount", "N/A"),
                        "Categories": ", ".join(volume_info.get("categories", ["N/A"])),
                        "AverageRating": volume_info.get("averageRating", "N/A"),
                        "RatingsCount": volume_info.get("ratingsCount", "N/A"),
                        "Language": volume_info.get("language", "N/A"),
                    }
                    books.append(book)
                start_index += max_per_request
                time.sleep(1)  # To avoid hitting the rate limit
            elif response.status_code == 429:
                logging.error("Rate limit hit. Saving fetched data and exiting.")
                return books
            else:
                logging.error(f"Failed to fetch data: {response.status_code}")
                break

    return books


def get_next_filename(directory, base_filename="random_books_data_", extension=".csv"):
    """
    Get the next available filename by incrementing the index.

    Args:
        directory (str): The directory where files are saved.
        base_filename (str): The base name of the file.
        extension (str): The file extension.

    Returns:
        str: The next available filename.
    """
    existing_files = os.listdir(directory)
    max_index = 0
    for filename in existing_files:
        if filename.startswith(base_filename) and filename.endswith(extension):
            try:
                index = int(filename[len(base_filename) : -len(extension)])
                if index > max_index:
                    max_index = index
            except ValueError:
                continue
    return f"{base_filename}{max_index + 1}{extension}"


def save_to_csv(
    books,
    directory=r"C:\Users\adame\OneDrive\Desktop\python_scripts\data_projects\google_books",
    base_filename="random_books_data_",
    extension=".csv",
):
    """
    Save books data to a CSV file.

    Args:
        books (list): A list of dictionaries containing book data.
        directory (str): The directory where the file will be saved.
        base_filename (str): The base name of the file.
        extension (str): The file extension.
    """
    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)

    filename = get_next_filename(directory, base_filename, extension)
    filepath = os.path.join(directory, filename)
    df = pd.DataFrame(books)
    df.to_csv(filepath, index=False)
    logging.info(f"Data saved to {filepath}")
    print(f"Data saved to {filepath}")


if __name__ == "__main__":
    try:
        logging.info("Script started.")
        # Fetch book data targeting 10,000 filtered books (or any number you prefer)
        books_list = fetch_books_data(10000)

        # Save the fetched books to CSV
        if books_list:
            save_to_csv(books_list)
        logging.info("Script finished successfully.")

    except KeyboardInterrupt:
        # Handle manual interruption (Ctrl+C)
        logging.warning("Program interrupted. Saving fetched data...")
        print("\nProgram interrupted. Saving fetched data...")
        if books_list:
            save_to_csv(books_list)
        logging.info("Data saved after interruption.")
