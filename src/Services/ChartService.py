import pandas as pd
import altair as alt
from vega_datasets import data
from iso3166 import countries as iso_countries
import math, logging
from sqlalchemy import text
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

from src.Services.PostgresService import PostgresService

class ChartService(PostgresService):
    """Handles the creation of Altair data visualizations."""

    def _get_iso_code(self, country_name):
        """Convert country name to ISO 3166-1 numeric code."""
        try:
            return iso_countries.get(country_name).numeric
        except (KeyError, AttributeError):
            return None

    def create_country_choropleth(self, df_country, color_scheme='blues'):
        """
        Generate a choropleth world map using Altair based on country-level record counts.

        Parameters:
        - df_country: DataFrame with columns ['Country', 'record_count']
        - color_scheme: Altair color scheme for the map (default: 'blues')

        Returns:
        - Altair Chart object
        """
        # Clean unknown country labels
        df_country['country'] = df_country['country'].replace('[]', 'Unknown')
        df_country.to_csv("testing_country.csv")
        # Convert country names to ISO numeric codes (for matching map IDs)
        def get_iso_code(name):
            try:
                return iso_countries.get(name).numeric
            except:
                return None

        df_country['id'] = df_country['country'].apply(get_iso_code)
        df_country = df_country.dropna(subset=['id']).copy()
        df_country['id'] = df_country['id'].astype(int)

        # Load TopoJSON world map
        countries = alt.topo_feature(data.world_110m.url, 'countries')

        # Create choropleth map
        world_map = (
            alt.Chart(countries)
            .mark_geoshape()
            .encode(
                color=alt.Color(
                    'record_count:Q',
                    title='Total Records',
                    # scale=alt.Scale(scheme=color_scheme)
                    scale=alt.Scale(
                        domain=[0, df_country['record_count'].max()],
                        range=[
                            "#bdd7e7",
                            "#08519c"
                        ]  # light grey → dark blue
                    )
                ),
                tooltip=[
                    alt.Tooltip('country:N', title='country'),
                    alt.Tooltip('record_count:Q', title='Total Records')
                ]
            )
            .transform_lookup(
                lookup='id',
                from_=alt.LookupData(df_country, 'id', ['country', 'record_count'])
            )
            .transform_calculate(
                record_count="datum.record_count !== null ? datum.record_count : 0"
            )
            .properties(
                width="container",
                height=450,
                title='Records by Country of Publication (Map)'
            )
            .project(
                type='naturalEarth1',
                scale=180,
                center=[0, 20],  # Shift map center (longitude, latitude)
                translate=[400, 200],
                # clipExtent=[[0, 0], [800, 350]]  # [x0, y0], [x1, y1] – crops lower part
            )
        )

        return world_map
   
    def generate_chart(self, data):
        """
        Generate charts from data with proper empty data handling
        """
        try:
            # ✨ Step 1: Create DataFrames with safe defaults
            df_year_source = pd.DataFrame(data.get('year_source', []))
            df_country = pd.DataFrame(data.get('country', []))
            df_journal = pd.DataFrame(data.get('journal', []))
            df_source = pd.DataFrame(data.get('source', []))
            
            # ✨ Step 2: Return empty charts if no data
            if df_year_source.empty and df_country.empty and df_journal.empty and df_source.empty:
                logging.warning("All data is empty, returning empty charts")
                return {
                    "year_source": self._empty_chart("No Year/Source Data Available"),
                    "country": self._empty_chart("No Country Data Available"),
                    "country_map": self._empty_chart("No Country Map Data Available"),
                    "journal": self._empty_chart("No Journal Data Available"),
                    "source": self._empty_chart("No Source Data Available")
                }
            
            # Colorblind-friendly palette
            colorblind_palette = "tableau20"
            okabe_ito_colors = ["#a6cee3", "#1f78b4", "#b2df8a", "#33a02c", "#0072B2", "#D55E00", "#CC79A7"]
            
            source_color_mapping = {
                "LOVE": "#a6cee3", 
                "OVID": "#1f78b4", 
                "Cochrane": "#b2df8a", 
                "Medline": "#33a02c"
            }
            source_domain = list(source_color_mapping.keys())
            source_range_ = list(source_color_mapping.values())
            
            # ✨ Chart 1: Year & Source bar chart (with empty check)
            if not df_year_source.empty and 'year' in df_year_source.columns and 'record_count' in df_year_source.columns:
                grouped_max = df_year_source.groupby(['year'])['record_count'].sum().max()
                y_max = math.ceil(grouped_max) if grouped_max > 0 else 10
                
                chart_year_source = (
                    alt.Chart(df_year_source)
                    .transform_calculate(
                        Year="datum.year !== null ? datum.year : 'Unknown'"
                    )
                    .mark_bar()
                    .encode(
                        x=alt.X('year:O', title='Year', axis=alt.Axis(labelAngle=-45)),
                        y=alt.Y(
                            'sum(record_count):Q', 
                            axis=alt.Axis(title='Total Records', tickMinStep=1, values=list(range(0, y_max + 1))),
                            scale=alt.Scale(domain=[0, y_max])
                        ),
                        color=alt.Color(
                            'source:N', 
                            legend=alt.Legend(orient="bottom", direction="horizontal"), 
                            title='Source', 
                            scale=alt.Scale(domain=source_domain, range=source_range_)
                        ),
                        tooltip=[
                            alt.Tooltip('year:O', title='Year'),
                            alt.Tooltip('source:N', title='Source'),
                            alt.Tooltip('sum(record_count):Q', title='Total Records')
                        ]
                    )
                    .properties(width="container", height=500, title='Cumulative Records Over Time by Source')
                    .configure(autosize="fit")
                    .interactive()
                )
            else:
                chart_year_source = self._empty_chart("No Year/Source Data Available")
            
            # ✨ Chart 2: Country bar chart (with empty check)
            if not df_country.empty and 'country' in df_country.columns:
                df_country['country'] = df_country['country'].replace('[]', 'Unknown')
                
                chart_country = (
                    alt.Chart(df_country)
                    .mark_bar()
                    .encode(
                        x=alt.X('country:N', title='Country', axis=alt.Axis(labelAngle=-45)),
                        y=alt.Y('record_count:Q', title='Total Records'),
                        color=alt.Color(
                            'country:N', 
                            legend=alt.Legend(orient="bottom", direction="horizontal"), 
                            scale=alt.Scale(scheme=colorblind_palette)
                        ),
                        tooltip=[
                            alt.Tooltip('country:N', title='Country'),
                            alt.Tooltip('record_count:Q', title='Total Records')
                        ]
                    )
                    .properties(width="container", height=300, title='Records by Country')
                    .configure(autosize="fit")
                    .interactive()
                )
                
                country_map = self.create_country_choropleth(df_country, color_scheme='blues')
            else:
                chart_country = self._empty_chart("No Country Data Available")
                country_map = self._empty_chart("No Country Map Data Available")
            
            # ✨ Chart 3: Journal bar chart (with empty check)
            if not df_journal.empty and 'journal' in df_journal.columns:
                chart_journal = (
                    alt.Chart(df_journal)
                    .mark_bar()
                    .encode(
                        x=alt.X('journal:N', title='Journal', axis=alt.Axis(labelAngle=-45)),
                        y=alt.Y('record_count:Q', title='Total Records'),
                        color=alt.Color(
                            'journal:N', 
                            legend=alt.Legend(orient="bottom", direction="horizontal"), 
                            scale=alt.Scale(domain=source_domain, range=source_range_)
                        ),
                        tooltip=[
                            alt.Tooltip('journal:N', title='Journal'),
                            alt.Tooltip('record_count:Q', title='Total Records')
                        ]
                    )
                    .properties(width="container", height=300, title='Records by Journal')
                    .configure(autosize="fit")
                    .interactive()
                )
            else:
                chart_journal = self._empty_chart("No Journal Data Available")
            
            # ✨ Chart 4: Source bar chart (with empty check)
            if not df_source.empty and 'source' in df_source.columns:
                chart_source = (
                    alt.Chart(df_source)
                    .mark_bar()
                    .encode(
                        x=alt.X('source:N', title='Source', axis=alt.Axis(labelAngle=-45)),
                        y=alt.Y('record_count:Q', title='Total Records'),
                        color=alt.Color(
                            'source:N', 
                            legend=alt.Legend(orient="bottom", direction="horizontal"), 
                            scale=alt.Scale(domain=df_source['source'].unique(), range=okabe_ito_colors)
                        ),
                        tooltip=[
                            alt.Tooltip('source:N', title='Source'),
                            alt.Tooltip('record_count:Q', title='Total Records')
                        ]
                    )
                    .properties(width="container", height=300, title='Records by Source')
                    .configure(autosize="fit")
                    .interactive()
                )
            else:
                chart_source = self._empty_chart("No Source Data Available")
            
            return {
                "year_source": chart_year_source.to_dict() if hasattr(chart_year_source, 'to_dict') else chart_year_source,
                "country": chart_country.to_dict() if hasattr(chart_country, 'to_dict') else chart_country,
                "country_map": country_map.to_dict() if hasattr(country_map, 'to_dict') else country_map,
                "journal": chart_journal.to_dict() if hasattr(chart_journal, 'to_dict') else chart_journal,
                "source": chart_source.to_dict() if hasattr(chart_source, 'to_dict') else chart_source
            }
            
        except Exception as e:
            logging.error(f"Error generating charts: {e}", exc_info=True)
            return {
                "year_source": self._empty_chart("Error generating chart"),
                "country": self._empty_chart("Error generating chart"),
                "country_map": self._empty_chart("Error generating chart"),
                "journal": self._empty_chart("Error generating chart"),
                "source": self._empty_chart("Error generating chart")
            }

    def _empty_chart(self, message="No Data Available"):
        """
        Create an empty placeholder chart
        """
        return {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "mark": "text",
            "encoding": {
                "text": {"value": message}
            },
            "config": {
                "view": {"strokeWidth": 0}
            },
            "width": "container",
            "height": 300
        }
        
    def get_summary_statistics(self):
        """
        Fetches summary statistics grouped individually and by Year for each feature: Country, Journal, and Source.
        Returns a dictionary containing grouped data.
        Dynamically checks for column existence to avoid errors.
        """
        try:
            with self.engine.connect() as connection:
                # ✨ Step 1: Check which columns exist
                check_columns_query = text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'all_db'
                """)
                
                available_columns_result = connection.execute(check_columns_query).fetchall()
                available_columns = {row[0].lower() for row in available_columns_result}
                
                logging.info(f"Available columns in all_db: {available_columns}")
                
                # ✨ Step 2: Build queries dynamically based on available columns
                queries = {}
                
                # Year-Source (if both year and source exist)
                if 'year' in available_columns and 'source' in available_columns:
                    queries["year_source"] = text("""
                        SELECT "year", "source", COUNT(*) as record_count
                        FROM all_db
                        WHERE "year" IS NOT NULL AND "source" IS NOT NULL
                        GROUP BY "year", "source"
                        ORDER BY "year", "source";
                    """)
                
                # Country (if exists)
                if 'country' in available_columns:
                    queries["country"] = text("""
                        SELECT "country", COUNT(*) as record_count
                        FROM all_db
                        WHERE "country" IS NOT NULL AND "country" != ''
                        GROUP BY "country"
                        ORDER BY record_count DESC;
                    """)
                
                # Journal (if exists)
                if 'journal' in available_columns:
                    queries["journal"] = text("""
                        SELECT "journal", COUNT(*) as record_count
                        FROM all_db
                        WHERE "journal" IS NOT NULL AND "journal" != ''
                        GROUP BY "journal"
                        ORDER BY record_count DESC
                        LIMIT 3;
                    """)
                
                # Source (if exists)
                if 'source' in available_columns:
                    queries["source"] = text("""
                        SELECT "source", COUNT(*) AS record_count
                        FROM all_db
                        WHERE "source" IS NOT NULL AND "source" != ''
                        GROUP BY "source"
                        ORDER BY record_count DESC
                        LIMIT 4;
                    """)
                
                # OVID Grouping (if both source and database exist)
                if 'source' in available_columns and 'database' in available_columns:
                    queries["ovid_grouping"] = text("""
                        SELECT "database", COUNT(*) AS record_count
                        FROM all_db
                        WHERE "source" = 'OVID' AND "database" IS NOT NULL AND "database" != ''
                        GROUP BY "database"
                        ORDER BY record_count DESC
                        LIMIT 4;
                    """)
                
                # ✨ Step 3: Execute each query and transform results into a dictionary
                summary_stats = {}
                for key, query in queries.items():
                    try:
                        result = connection.execute(query).fetchall()
                        summary_stats[key] = [
                            dict(row._mapping) for row in result
                        ]
                        logging.info(f"Query '{key}' returned {len(summary_stats[key])} rows")
                    except Exception as query_error:
                        logging.error(f"Error executing query '{key}': {query_error}")
                        summary_stats[key] = []  # Safe default
                
                # ✨ Step 4: Add empty arrays for missing queries
                # This ensures frontend always receives expected structure
                expected_keys = ["year_source", "country", "journal", "source", "ovid_grouping"]
                for key in expected_keys:
                    if key not in summary_stats:
                        summary_stats[key] = []
                        logging.info(f"Column for '{key}' not available, returning empty array")
                
                # Generate charts
                charts = self.generate_chart(summary_stats)
                
                return {
                    "status": "success",
                    "data": summary_stats,
                    "charts": charts
                }

        except Exception as e:
            logging.error(f"Error fetching summary statistics: {e}", exc_info=True)
            # Return safe structure
            return {
                "status": "error",
                "message": str(e),
                "data": {
                    "year_source": [],
                    "country": [],
                    "journal": [],
                    "source": [],
                    "ovid_grouping": []
                },
                "charts": []
            }
