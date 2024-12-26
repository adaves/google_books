import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import fnmatch


def load_data(directory, search_string):
    """
    This function looks for downloaded data from Google Books. The data is then loaded into a dictionary of dataframes.

    Parameters:
    directory (str): The directory to look in for CSV files.
    search_string (str): The name pattern of the downloaded files.

    Returns:
    dict: A dictionary containing dataframes of the loaded CSV files.
    """
    dataframes = {}
    counter = 1

    for root, dirs, files in os.walk(directory):
        for file in files:
            if fnmatch.fnmatch(file.lower(), f"*{search_string.lower()}*"):
                df = pd.read_csv(os.path.join(root, file))
                dataframes[f"df_{counter}"] = df
                counter += 1

    return dataframes


def plot_missing_values(dataframe):
    """
    Check for missing values and plot a simple histogram of counts of missing values in each column.

    Parameters:
    dataframe (pd.DataFrame): The dataframe to check for missing values.

    Returns:
    None
    """
    dataframe.isna().sum().plot(
        kind="bar",
        title="Count of missing values in each column",
        xlabel="Columns",
        ylabel="Count of missing values",
        figsize=(10, 6),
        color="skyblue",
        rot=45,
        fontsize=12,
    )
    plt.show()


def change_datatypes(dataframe):
    """
    Change the data types of certain columns to types more appropriate for machine learning.

    Parameters:
    dataframe (pd.DataFrame): The input dataframe.

    Returns:
    pd.DataFrame: The dataframe with changed column data types.
    """
    dataframe["PageCount"] = dataframe["PageCount"].astype(int)
    dataframe["RatingsCount"] = dataframe["RatingsCount"].astype(int)
    dataframe["ISBN"] = (
        dataframe["ISBN"].astype(str).apply(lambda x: x.replace(".0", ""))
    )
    return dataframe


def fix_dates(dataframe):
    """
    Standardize the date column and ensure all date columns are in the same format.

    Parameters:
    dataframe (pd.DataFrame): The input dataframe.

    Returns:
    pd.DataFrame: The dataframe with standardized date columns.
    """
    if (
        not dataframe["PublishedDate"]
        .astype(str)
        .str.contains(r"\d{4}-\d{2}-\d{2}")
        .all()
    ):
        year_only = dataframe["PublishedDate"].str.match(r"^\d{4}$")
        dataframe.loc[year_only, "PublishedDate"] = (
            dataframe.loc[year_only, "PublishedDate"] + "-01-01"
        )

    dataframe["PublishedDate"] = pd.to_datetime(
        dataframe["PublishedDate"], errors="coerce"
    )
    return dataframe


def inspect_isbn_column(dataframe):
    """
    Inspect the dataframe and remove rows with duplicate ISBN numbers.

    Parameters:
    dataframe (pd.DataFrame): The input dataframe.

    Returns:
    pd.DataFrame: The dataframe with duplicate ISBN rows removed.
    """
    dataframe = dataframe.drop_duplicates(subset="ISBN")
    return dataframe


def add_features(dataframe):
    """
    Add new columns to the dataframe.

    Parameters:
    dataframe (pd.DataFrame): The input dataframe.

    Returns:
    pd.DataFrame: The dataframe with new columns added.
    """
    dataframe["TitleWordCount"] = dataframe["Title"].str.split().apply(len)
    return dataframe


if __name__ == "__main__":
    # Load data
    dataframes_dict = load_data(
        r"C:\Users\adame\OneDrive\Desktop\python_scripts\data_projects\google_books",
        "random_books",
    )
    dataframes = list(dataframes_dict.values())
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Plot missing values
    plot_missing_values(combined_df)

    # Drop missing values
    combined_df.dropna(inplace=True)

    # Change data types
    combined_df = change_datatypes(combined_df)

    # Drop rows with PageCount == 0
    combined_df = combined_df[combined_df["PageCount"] != 0]

    # Fix dates
    combined_df = fix_dates(combined_df)

    # Inspect and remove duplicate ISBNs
    combined_df = inspect_isbn_column(combined_df)

    # Add new features
    combined_df = add_features(combined_df)

    # Print the first 10 rows of the dataframe
    print(combined_df.head(10))

    # Save the dataframe to CSV
    combined_df.to_csv(
        r"C:\Users\adame\OneDrive\Desktop\python_scripts\data_projects\google_books\data\processed\combined_books_mark-II.csv",
        index=False,
    )
