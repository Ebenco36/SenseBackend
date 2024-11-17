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
                self.dataframes.append(df)
            except Exception as e:
                print(f"Error loading {path}: {e}")

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
        
        
csv_sources = {
    "Cochrane/cochrane_combined_output.csv": "Cochrane",
    "MedlineData/medline_results.csv": "Medline",
    "OVIDNew/merged_journal_data_2024-11-13.csv": "OVID",
    "L-OVE/LOVE.csv": "LOVE"
}

common_columns = ['Id', 'Title', 'Authors', 'DOI']

rename_maps = {
    "Cochrane/cochrane_combined_output.csv": {
        "cdIdentifier": "Id",
        "title": "Title",
        "doi_link": "DOI",
        "doi": "DOI_only",
        "modifiedDate": "Date",
        "resultType": "Result_type",
        "resultStage": "Result_stage",
        "authors": "Authors",
        "patient_population": "Cochrane_patient_population",
        "intervention": "Cochrane_intervention",
        "comparator": "Cochrane_comparator",
        "outcomes": "Cochrane_outcomes",
        "abstract": "Abstract"                     
    },
    "MedlineData/medline_results.csv": {
        "PMID": "Id",
        "Publication Date": "Date",
        "Source": "Medline_source"
    },
    "OVIDNew/merged_journal_data_2024-11-13.csv": {
        # Custom renaming for OVID if needed
    },
    "L-OVE/LOVE.csv": {
        "id": "Id",
        "authors": "Authors",
        "classification": "Classification",
        "doi": "DOI",
        "year": "Year",
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