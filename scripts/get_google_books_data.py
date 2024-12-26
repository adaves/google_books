# script to get googgle book data

import requests
import random
import string
import time
import pandas as pd
import os
from dotenv import load_dotenv

# load environment variables from .env file
load_dotenv()

# GET API KEY FROM .env file
API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")

if not API_KEY:
    raise ValueError("API Key not found. Please set it in your .env file.")


# Function to generate a random search query (e.g., a random letter)
def get_random_query():
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(1))  # Single random letter


# Function to fetch book data from Google Books API with pagination
def fetch_books_data(target_count=10000):
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

                # Parse and add the books from this page
                books.extend(parse_books_data(data))
                print(f"Fetched {len(books)} books so far.")

                # Stop if we've reached the target count
                if len(books) >= target_count:
                    break

                # Update start_index for next batch of results
                start_index += max_per_request

            elif response.status_code == 429:
                # Handle rate limiting by waiting before retrying with use input

                user_input = (
                    input(
                        "Rate limit exceeded. End program or continue to wait? Enter 'end' or 'wait': "
                    )
                    .strip()
                    .lower()
                )

                if user_input == "end":
                    print("Ending program and saving fetched data...")
                    return books
                elif user_input == "wait":
                    print("Waiting for 60 seconds before retrying...")
                    time.sleep(60)  # Wait before retrying
                else:
                    print("Invalid input. Continue to wait.")
                    time.sleep(60)  # default behavior if input is invalid

            else:
                print(f"Error fetching data: {response.status_code}")
                break  # Stop on other errors

    return books[:target_count]  # Return only up to the target count


# Function to parse book data and convert it into a list of dictionaries
def parse_books_data(data):
    books = []
    for item in data.get("items", []):
        volume_info = item.get("volumeInfo", {})
        book = {
            "Title": volume_info.get("title", "N/A"),
            "Authors": ", ".join(volume_info.get("authors", ["N/A"])),
            "Publisher": volume_info.get("publisher", "N/A"),
            "PublishedDate": volume_info.get("publishedDate", "N/A"),
            "ISBN": next(
                (
                    identifier["identifier"]
                    for identifier in volume_info.get("industryIdentifiers", [])
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
    return books


# Fetch book data targeting books
books_list = fetch_books_data(10000)


# Function to save books data to CSV
def save_to_csv(books, filename="random_books_data_13.csv"):
    df = pd.DataFrame(books)
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")


# Save the fetched books to CSV
if books_list:
    save_to_csv(books_list)
