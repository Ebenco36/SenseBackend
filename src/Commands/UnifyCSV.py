import sys
import os
import re
import csv
from typing import List, Dict, Set
from datetime import datetime

# Set CSV field size limit
try:
    max_int = sys.maxsize
    while True:
        try:
            csv.field_size_limit(max_int)
            break
        except OverflowError:
            max_int = int(max_int / 10)
except Exception:
    csv.field_size_limit(1000000)

class CSVUnifier:
    def __init__(self, csv_sources: Dict[str, str], common_columns: List[str] = None, rename_maps: Dict[str, Dict[str, str]] = None):
        self.csv_sources = csv_sources
        self.common_columns = common_columns if common_columns else []
        self.rename_maps = rename_maps if rename_maps else {}

    @staticmethod
    def _normalize_doi(doi: str) -> str:
        if not doi or isinstance(doi, (float, int)) and not doi:
            return ""
        return re.sub(r'^(https?://)?(dx\.)?doi\.org/', '', str(doi).lower()).strip()

    def _get_all_headers(self) -> List[str]:
        allowed_columns: Set[str] = set(self.common_columns)
        for rename_map in self.rename_maps.values():
            allowed_columns.update(rename_map.values())
        allowed_columns.update(['id', 'verification_id', 'source', 'created_at', 'cleaned_doi'])
        ordered_cols = [col for col in self.common_columns if col in allowed_columns]
        ordered_cols += sorted(col for col in allowed_columns if col not in ordered_cols)
        return ordered_cols

    def process_and_save(self, unique_output_path: str, duplicate_output_path: str):
        print("Starting unification process...")
        os.makedirs(os.path.dirname(unique_output_path), exist_ok=True)
        os.makedirs(os.path.dirname(duplicate_output_path), exist_ok=True)

        all_headers = self._get_all_headers()
        seen_dois: Set[str] = set()
        unique_count = 0
        duplicate_count = 0

        try:
            with open(unique_output_path, 'w', newline='', encoding='utf-8') as unique_file, \
                 open(duplicate_output_path, 'w', newline='', encoding='utf-8') as duplicate_file:

                unique_writer = csv.DictWriter(unique_file, fieldnames=all_headers, extrasaction='ignore')
                duplicate_writer = csv.DictWriter(duplicate_file, fieldnames=all_headers, extrasaction='ignore')
                unique_writer.writeheader()
                duplicate_writer.writeheader()

                for path, source_name in self.csv_sources.items():
                    print(f"Processing file: {path}")
                    row_counter = 1
                    rename_map = self.rename_maps.get(path, {})

                    try:
                        with open(path, 'r', encoding='utf-8') as infile:
                            reader = csv.DictReader(infile)

                            for row in reader:
                                renamed_row = {}

                                for k, v in row.items():
                                    if k in rename_map:
                                        renamed_col = rename_map[k]
                                        renamed_row[renamed_col] = v
                                    elif k in self.common_columns:
                                        renamed_row[k] = v

                                renamed_row['id'] = f"{source_name}_{row_counter}"
                                renamed_row['source'] = source_name
                                renamed_row['created_at'] = datetime.now().isoformat()
                                row_counter += 1

                                doi = renamed_row.get('doi', '')
                                cleaned_doi = self._normalize_doi(doi)
                                renamed_row['cleaned_doi'] = cleaned_doi

                                if not cleaned_doi:
                                    unique_writer.writerow(renamed_row)
                                    unique_count += 1
                                elif cleaned_doi in seen_dois:
                                    duplicate_writer.writerow(renamed_row)
                                    duplicate_count += 1
                                else:
                                    seen_dois.add(cleaned_doi)
                                    unique_writer.writerow(renamed_row)
                                    unique_count += 1

                    except FileNotFoundError:
                        print(f"⚠️ Warning: File not found and will be skipped: {path}")
                    except csv.Error as e:
                        print(f"❌ CSV formatting error in {path}: {e}")
                    except Exception as e:
                        print(f"❌ Unexpected error in {path}: {e}")

            print("\n--- Process Complete ---")
            print(f"✅ Saved {unique_count} unique records to {unique_output_path}")
            if duplicate_count > 0:
                print(f"ℹ️  Found and saved {duplicate_count} duplicate records to {duplicate_output_path}")
            else:
                print("ℹ️  No duplicate records were found.")

        except Exception as e:
            print(f"❌ Critical error during file writing: {e}")

    @staticmethod
    def extract_column_duplicates(input_file: str, output_file: str, column_to_check: str):
        """
        After unification, extract rows where 'column_to_check' has duplicates.
        """
        print(f"\n🔍 Extracting duplicates in column '{column_to_check}' from '{input_file}'...")

        value_counts = {}
        rows = []

        with open(input_file, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                col_value = row.get(column_to_check)
                if col_value:
                    value_counts[col_value] = value_counts.get(col_value, 0) + 1
                rows.append(row)

        duplicate_values = {k for k, v in value_counts.items() if v > 1}

        count = 0
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in rows:
                if row[column_to_check] in duplicate_values:
                    writer.writerow(row)
                    count += 1

        print(f"✅ Duplicated rows by '{column_to_check}' saved to {output_file} ({count} rows).")

def get_latest_file(directory: str, prefix: str, suffix: str) -> str | None:
    latest_file = None
    latest_date = None
    if not os.path.exists(directory):
        return None
    for filename in os.listdir(directory):
        if filename.startswith(prefix) and filename.endswith(suffix):
            date_str = filename[len(prefix):-len(suffix)]
            try:
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
                if latest_date is None or file_date > latest_date:
                    latest_date = file_date
                    latest_file = filename
            except ValueError:
                print(f"⚠️ Warning: Skipped file with invalid date format: {filename}")
                continue
    return os.path.join(directory, latest_file) if latest_file else None

# --- Script Execution ---
if __name__ == "__main__":
    ovid_dir = "./Data/OVIDNew/"
    ovid_prefix = "merged_journal_data_"
    ovid_suffix = ".csv"
    latest_ovid_file = get_latest_file(ovid_dir, ovid_prefix, ovid_suffix)

    csv_sources = {
        "Data/Cochrane/cochrane_combined_output_enriched.csv": "Cochrane",
        "Data/MedlineData/medline_results.csv": "Medline",
        "Data/L-OVE/LOVE.csv": "LOVE"
    }
    if latest_ovid_file:
        csv_sources[latest_ovid_file] = "OVID"
    else:
        print(f"⚠️ Warning: No OVID file found in '{ovid_dir}'. Skipping.")

    common_columns = ['id', 'verification_id', 'title', 'authors', 'doi', 'source', 'cleaned_doi', 'created_at']

    rename_maps = {
        "Data/Cochrane/cochrane_combined_output_enriched.csv": {
            "cd_identifier": "verification_id", "title": "title", "doi": "doi",
            "authors": "authors", "modified_date": "date", "result_type": "result_type",
            "result_stage": "publication_type", "abstract": "abstract", "journal": "journal",
            "open_access": "open_access", "pdf_url": "pdf_url", "language": "language",
            "country": "country", "year": "year", "publisher": "publisher"
        },
        "Data/MedlineData/medline_results.csv": {
            "pmid": "verification_id", "title": "title", "abstract": "abstract", "authors": "authors",
            "publication_date": "date", "journal": "journal", "country": "country",
            "publication_type": "publication_type", "doi": "doi", "open_access": "open_access",
            "language": "language", "year": "year"
        },
        "Data/L-OVE/LOVE.csv": {
            "id": "verification_id", "authors": "authors", "classification": "publication_type",
            "doi": "doi", "title": "title", "abstract": "abstract",
            "countries": "country", "journal": "journal",
            "study_design": "study_design", "keywords": "keywords",
            "year": "year"
        }
    }
    if latest_ovid_file:
        rename_maps[latest_ovid_file] = {
            "publisher": "publisher", "date_delivered": "date", "publication_accession_number": "verification_id",
            "publication_type": "publication_type", "journal": "journal", "database": "database",
            "authors": "authors", "title_link": "title_link", "title": "title", "abstract": "abstract",
            "doi": "doi", "institution": "institution", "country": "country", "language": "language",
            "emtree_headings": "keywords", "year": "year"
        }

    unifier = CSVUnifier(csv_sources, common_columns=common_columns, rename_maps=rename_maps)
    unifier.process_and_save(
        unique_output_path="Data/output/unified_output.csv",
        duplicate_output_path="Data/output/duplicates_output.csv"
    )

    # Extract verification_id duplicates AFTER unification
    CSVUnifier.extract_column_duplicates(
        input_file="Data/output/unified_output.csv",
        output_file="Data/output/duplicated_rows_by_verification_id.csv",
        column_to_check="verification_id"
    )
