import logging
import os

import pandas as pd
from src.Journals.Services.services import JSONService
from src.Services.PostgresService import PostgresService
from sqlalchemy import text

class VisualizationService:
    def __init__(self):
        self.db = PostgresService()
        self.json_service = JSONService()

    def get_filter_data(self):
        """
        Fetches the min/max years and all unique topics for the filter controls,
        correctly deriving topics from the complex tag filter structure.
        """
        try:
            # Step 1: Get all years from the database to calculate min/max.
            all_years_raw = self.db.get_unique_items_from_column("all_db", "year")
            all_years = [int(y) for y in all_years_raw if y is not None]

            # Step 2: Generate the complex tag filter structure, just like FilterAPI.
            all_columns = self.db.get_column_names("all_db")
            # Assuming json_service has the process_columns_with_hash method
            tag_filter_response = self.json_service.get_all_filter_options()
            tag_filters = tag_filter_response.get("data", {}).get("tag_filters", {})
            # Step 3: Extract the topic names from the nested structure.
            all_topics = []
            if 'topic' in tag_filters:
                # The keys of the 'topic' dictionary are the topic names
                # e.g., ["acceptance", "adm", "coverage", "eco", ...]
                all_topics = list(tag_filters['topic'].keys())

            # Step 4: Assemble and return the final response.
            return {
                "minYear": min(all_years) if all_years else 2000,
                "maxYear": max(all_years) if all_years else 2025,
                "allTopics": sorted(all_topics)
            }
        except Exception as e:
            logging.error(f"Failed to get filter data: {e}")
            # Return a safe default structure on failure
            return {
                "minYear": 2000,
                "maxYear": 2025,
                "allTopics": []
            }

    def get_dashboard_data(self, start_year=None, end_year=None, topic=None):
        """
        Runs a single, powerful SQL query to aggregate all data for the dashboard,
        using all lowercase column names to match the database schema.
        """
        try:
            # Step 1: Get the mapping from friendly topic names to DB column names
            all_filters_config = self.json_service.get_all_filter_options().get("data", {})
            category_to_column_map = self.json_service._generate_filter_category_mappings(all_filters_config)

            # Step 2: Build the WHERE clause and parameters dynamically
            where_clauses = []
            params = {}

            # ✅ FIX: Use lowercase "year" column
            if start_year is not None:
                where_clauses.append('"year" >= :start_year')
                params['start_year'] = start_year
            if end_year is not None:
                where_clauses.append('"year" <= :end_year')
                params['end_year'] = end_year

            if topic:
                topic_db_column = category_to_column_map.get(topic)
                if topic_db_column:
                    # The HASH columns are already lowercase from the generation logic
                    where_clauses.append(f'"{topic_db_column}" IS NOT NULL')
                else:
                    logging.warning(f"Invalid topic '{topic}' provided; it will be ignored.")
            
            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            
            # Step 3: Define the main aggregation query with all lowercase column names
            final_query = text(f"""
                WITH filtered_reviews AS (
                    SELECT * FROM all_db
                    {where_sql}
                ),
                year_source_agg AS (
                    SELECT json_agg(json_build_object('year', "year", 'source', "source", 'record_count', record_count)) AS data
                    FROM (
                        SELECT "year", "source", COUNT(*) AS record_count FROM filtered_reviews GROUP BY "year", "source"
                    ) AS yearly_counts
                ),
                country_agg AS (
                    SELECT json_agg(json_build_object('country', "country", 'record_count', record_count)) AS data
                    FROM (
                        SELECT "country", COUNT(*) AS record_count FROM filtered_reviews WHERE "country" IS NOT NULL AND "country" != '' GROUP BY "country" ORDER BY record_count DESC
                    ) AS country_counts
                ),
                journal_agg AS (
                    SELECT json_agg(json_build_object('journal', "journal", 'record_count', record_count)) AS data
                    FROM (
                        SELECT "journal", COUNT(*) AS record_count FROM filtered_reviews WHERE "journal" IS NOT NULL GROUP BY "journal" ORDER BY record_count DESC LIMIT 3
                    ) AS journal_counts
                ),
                source_agg AS (
                    SELECT json_agg(json_build_object('source', "source", 'record_count', record_count)) AS data
                    FROM (
                        SELECT "source", COUNT(*) AS record_count
                        FROM filtered_reviews
                        WHERE "source" IS NOT NULL AND "source" != ''
                        GROUP BY "source"
                        ORDER BY record_count DESC
                        LIMIT 4
                    ) AS source_counts
                ),
                ovid_grouping_agg AS (
                    SELECT json_agg(json_build_object('database', "database", 'record_count', record_count)) AS data
                    FROM (
                        SELECT "database", COUNT(*) AS record_count FROM filtered_reviews WHERE "source" = 'OVID' AND "database" IS NOT NULL GROUP BY "database" ORDER BY record_count DESC LIMIT 4
                    ) AS ovid_counts
                )
                -- Final step: Combine all JSON aggregates into a single row
                SELECT
                    (SELECT data FROM year_source_agg) AS "year_source",
                    (SELECT data FROM country_agg) AS "country",
                    (SELECT data FROM journal_agg) AS "journal",
                    (SELECT data FROM source_agg) AS "source",
                    (SELECT data FROM ovid_grouping_agg) AS "ovid_grouping"
            """)
            
            # Step 4: Execute the query and return the result
            result = self.db.execute_raw_query(final_query, params)
            
            # --- Step 3: Load duplicates from CSV files ---
            verification_duplicates_file = './Data/output/duplicated_rows_by_verification_id.csv'
            doi_duplicates_file = './Data/output/duplicates_output.csv'

            verification_df = pd.DataFrame()
            doi_df = pd.DataFrame()

            if os.path.exists(verification_duplicates_file):
                verification_df = pd.read_csv(verification_duplicates_file)

            if os.path.exists(doi_duplicates_file):
                doi_df = pd.read_csv(doi_duplicates_file)
                
            # Optional filtering by year/ (for CSV data)
            verification_df = self.apply_filters(verification_df, start_year, end_year)
            doi_df = self.apply_filters(doi_df, start_year, end_year)
            
            if not verification_df.empty:
                verification_grouped = (
                    verification_df.groupby('source')
                    .size()
                    .reset_index(name='count')
                    .to_dict(orient='records')
                )
            else:
                verification_grouped = []

            if not doi_df.empty:
                doi_grouped = (
                    doi_df.groupby('source')
                    .size()
                    .reset_index(name='count')
                    .to_dict(orient='records')
                )
            else:
                doi_grouped = []
                
            summary_stats = self.json_service.get_summary_statistics()

            data = {
                "data": result[0] if result else {},
                "verification_duplicates": verification_grouped,
                "doi_duplicates": doi_grouped,
                "summary_statistics": summary_stats
            }
            return data

        except Exception as e:
            logging.error(f"Failed to get dashboard data: {e}")
            return {}

    def apply_filters(self, df, start_year, end_year):
        """
        Applies year and topic filtering to a DataFrame.
        """
        if df.empty:
            return df

        if 'year' in df.columns:
            if start_year:
                df = df[df['year'] >= start_year]
            if end_year:
                df = df[df['year'] <= end_year]

        return df