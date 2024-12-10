import sys
import os
sys.path.append(os.getcwd())
import pandas as pd
from typing import List, Dict

class CSVUnifier:
    def __init__(self, csv_sources: Dict[str, str], common_columns: List[str] = None, rename_maps: Dict[str, Dict[str, str]] = None):
        """
        Initializes the CSVUnifier with paths to CSV files and corresponding source names, 
        common columns, and optional column rename mappings.
        
        :param csv_sources: A dictionary with file paths as keys and source names as values.
        :param common_columns: List of columns to prioritize as common. If None, all overlapping columns are used.
        :param rename_maps: A dictionary where keys are CSV file paths and values are dictionaries with column rename mappings.
        """
        self.csv_sources = csv_sources
        self.common_columns = common_columns
        self.rename_maps = rename_maps if rename_maps else {}
        self.dataframes = []

    def load_and_rename_csvs(self):
        """Loads CSVs, renames columns based on rename_maps, adds a source column, and appends to the dataframes list."""
        for path, source_name in self.csv_sources.items():
            try:
                df = pd.read_csv(path)
                # Apply renaming if specified
                if path in self.rename_maps:
                    df.rename(columns=self.rename_maps[path], inplace=True)
                
                # Add a source column with the specified source name
                df['Source'] = source_name
                
                # Check if 'Id' column exists, otherwise create one
                if 'Id' not in df.columns and 'id' not in df.columns:
                    df['Id'] = [f"{source_name}_{i+1}" for i in range(len(df))]
                    
                # Extended mapping dictionary for language codes
                language_map = {
                    "eng": "English", "chi": "Chinese", "spa": "Spanish", "fre": "French",
                    "ger": "German", "jpn": "Japanese", "tur": "Turkish", "rus": "Russian",
                    "ita": "Italian", "hun": "Hungarian", "swe": "Swedish", "ara": "Arabic",
                    "kor": "Korean", "por": "Portuguese", "hin": "Hindi", "heb": "Hebrew",
                    "dut": "Dutch", "dan": "Danish", "nor": "Norwegian", "pol": "Polish",
                    "tha": "Thai", "vie": "Vietnamese", "gre": "Greek", "ukr": "Ukrainian",
                    "cze": "Czech", "rom": "Romanian", "tam": "Tamil", "ben": "Bengali"
                }
                if 'language' in df.columns:
                    # Replace abbreviations with full forms
                    df["language"] = df["language"].map(language_map).fillna("Unknown")
                # Extract year from Date column if it exists
                if 'publication_year' in df.columns and 'year' in df.columns:
                    # Use 'publication_year' if available, otherwise fallback to 'year'
                    df['Year'] = df['publication_year'].fillna(df['year']).fillna(-1).astype(int)
                    # df.rename(columns={"year": "Year"}, inplace=True)
                elif 'publication_year' in df.columns:
                    # Only 'publication_year' exists
                    df['Year'] = df['publication_year'].fillna(-1).astype(int)
                elif 'year' in df.columns:
                    # Only 'year' exists
                    df['Year'] = df['year'].astype(int)
                    # df.rename(columns={"year": "Year"}, inplace=True)
                elif 'Date' in df.columns:
                    # Extract year from 'Date' column
                    df['Year'] = self.extract_year(df['Date']).fillna(-1).astype(int)
                else:
                    # Neither column exists, create 'Year' with NaN values
                    df['Year'] = pd.NA
                    
                self.dataframes.append(df)
            except Exception as e:
                print(f"Error loading {path}: {e}")

    @staticmethod
    def extract_year(date_series: pd.Series) -> pd.Series:
        """Extracts the year from a series of dates in various formats."""
        try:
            # Define possible date formats to try
            date_formats = [
                '%Y %b %d',  # e.g., '2024 Dec 1'
                '%d %B %Y',  # e.g., '17 December 2020'
                '%Y',        # e.g., '2024'
            ]

            # Initialize a series with NaT
            parsed_dates = pd.Series(pd.NaT, index=date_series.index)

            # Attempt parsing with each format
            for date_format in date_formats:
                unparsed = parsed_dates.isna()  # Identify unparsed dates
                parsed_dates[unparsed] = pd.to_datetime(
                    date_series[unparsed], format=date_format, errors='coerce'
                )

            # Extract the year
            years = parsed_dates.dt.year

            # Ensure integers for valid years and NaN for missing values
            return years.astype('Int64')  # Nullable integer type
        except Exception as e:
            print(f"Error extracting year: {e}")
            return pd.Series([None] * len(date_series), dtype='Int64')

        
        
    def get_combined_columns(self) -> List[str]:
        """Determines combined columns from all DataFrames, prioritizing common columns."""
        all_columns = set()
        for df in self.dataframes:
            all_columns.update(df.columns)
        # Prioritize common columns if specified, otherwise use all overlapping columns
        if self.common_columns:
            combined_columns = [col for col in self.common_columns if col in all_columns]
            combined_columns += [col for col in all_columns if col not in combined_columns]
        else:
            combined_columns = list(all_columns)
        return combined_columns

    def unify_data(self) -> pd.DataFrame:
        """Unifies data from multiple CSV files into a single DataFrame with combined columns and deduplication by 'Id'."""
        combined_columns = self.get_combined_columns()
        
        unified_data = pd.DataFrame(columns=combined_columns)
        existing_ids = set()

        for df in self.dataframes:
            # Standardize columns
            df = df.reindex(columns=combined_columns)

            # Filter out duplicate records based on 'Id' column
            if 'Id' in df.columns:
                df = df[~df['Id'].isin(existing_ids)]
                existing_ids.update(df['Id'].dropna().unique())

            # Append non-duplicate records to the unified DataFrame
            unified_data = pd.concat([unified_data, df], ignore_index=True, sort=False)

        return unified_data

    def save_unified_csv(self, output_path: str):
        """Saves the unified DataFrame to a CSV file."""
        self.load_and_rename_csvs()
        unified_data = self.unify_data()
        unified_data.to_csv(output_path, index=False)
        print(f"Unified CSV saved to {output_path}")
        
from datetime import datetime
def get_latest_file(directory, prefix, suffix):
    """
    Get the latest file in a directory based on a date in the filename.

    :param directory: Path to the directory to search for files.
    :param prefix: The prefix of the filename to match (e.g., "merged_journal_data_").
    :param suffix: The suffix or file extension to match (e.g., ".csv").
    :return: The full path of the latest file or None if no matching file is found.
    """
    latest_file = None
    latest_date = None

    for filename in os.listdir(directory):
        if filename.startswith(prefix) and filename.endswith(suffix):
            # Extract the date part of the filename
            date_str = filename[len(prefix):-len(suffix)]
            try:
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
                if latest_date is None or file_date > latest_date:
                    latest_date = file_date
                    latest_file = filename
            except ValueError:
                # Skip files with invalid date formats
                continue

    if latest_file:
        return os.path.join(directory, latest_file)
    return None

directory = f"./OVIDNew/"
prefix = "merged_journal_data_"
suffix = ".csv"

latest_file = get_latest_file(directory, prefix, suffix)
     
csv_sources = {
    "Cochrane/cochrane_combined_output.csv": "Cochrane",
    "MedlineData/medline_results.csv": "Medline",
    f"{latest_file}": "OVID",
    "L-OVE/LOVE.csv": "LOVE"
}

common_columns = ['Id', 'Title', 'Authors', 'DOI']

rename_maps = {
    "Cochrane/cochrane_combined_output.csv": {
        "cdIdentifier": "other_id",
        "title": "Title",
        "doi_link": "DOI",
        "doi": "DOI_only",
        "modifiedDate": "Date",
        "resultType": "Result_type",
        "resultStage": "Publication_type",
        "authors": "Authors",
        "patient_population": "Cochrane_patient_population",
        "intervention": "Cochrane_intervention",
        "comparator": "Cochrane_comparator",
        "outcomes": "Cochrane_outcomes",
        "abstract": "Abstract",
        "journal": "Journal",
        "open_access": "Open_access"                 
    },
    "MedlineData/medline_results.csv": {
        "pmid": "other_id",
        "title": "Title",
        "abstract": "Abstract",
        "authors": "Authors",
        "publication_date": "Date",
        "journal": "Journal",
        "country": "Country", 
        "publication_type": "Publication_type",
        "doi": "DOI",
        "open_access": "Open_access"   
    },
    f"{latest_file}": {
        "PublicationType": "Publication_type",
        "DateDelivered": "Date"
    },
    "L-OVE/LOVE.csv": {
        "id": "other_id",
        "authors": "Authors",
        "classification": "Classification",
        "doi": "DOI",
        "publication_type": "Publication_type",
        "title": "Title",
        "abstract": "Abstract",
        "clinicaltrials": "Clinical_trials",
        "registry_of_trials": "Registry_of_trials",
        "countries": "Country",
        "links": "Links",
        "journal": "Journal",
        "study_design": "Study_design",
        "keywords": "Keywords"
    }
}

unifier = CSVUnifier(csv_sources, common_columns=common_columns, rename_maps=rename_maps)
unifier.save_unified_csv("output/unified_output.csv")