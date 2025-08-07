import pandas as pd
import altair as alt
from vega_datasets import data
from iso3166 import countries as iso_countries
import math

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
        df_year_source = pd.DataFrame(data['year_source'])
        df_country = pd.DataFrame(data['country'])
        df_journal = pd.DataFrame(data['journal'])
        df_source = pd.DataFrame(data['source'])
        
        # Colorblind-friendly palette

        colorblind_palette = "tableau20"  # Alternative: "viridis", "set2"
        okabe_ito_colors = ["#a6cee3", "#1f78b4", "#b2df8a", "#33a02c", "#0072B2", "#D55E00", "#CC79A7"]
        # Compute dynamic y-axis max
        grouped_max = df_year_source.groupby(['year'])['record_count'].sum().max()
        y_max = math.ceil(grouped_max)
        source_color_mapping = {
            "LOVE": "#a6cee3", 
            "OVID": "#1f78b4", 
            "Cochrane": "#b2df8a", 
            "Medline": "#33a02c"
        }
        source_domain = list(source_color_mapping.keys())  # List of categories
        source_range_ = list(source_color_mapping.values())  # Corresponding colors

        # Year & Source bar chart
        chart_year_source = (
            alt.Chart(df_year_source)
            .transform_calculate(
                Year="datum.year !== null ? datum.year : 'Unknown'"  # Replaces null with "Unknown"
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
                    scale=alt.Scale(
                        domain=source_domain, 
                        range=source_range_
                    )
                ), #scheme=colorblind_palette
                tooltip=[
                    alt.Tooltip('year:O', title='Year'),
                    alt.Tooltip('source:N', title='Source'),
                    alt.Tooltip('sum(record_count):Q', title='Total Records')
                ]
            )
            .properties(width="container", height=500, title='Cummulative Records Over Time by Source')
            .configure(autosize="fit")
            .interactive()
        )

        # Country bar chart
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
            .configure(
                autosize="fit"
            ).interactive()
        )

        country_map = self.create_country_choropleth(df_country, color_scheme='blues')
        
        # Journal bar chart
        chart_journal = (
            alt.Chart(df_journal)
            .mark_bar()
            .encode(
                x=alt.X('journal:N', title='Journal', axis=alt.Axis(labelAngle=-45)),
                y=alt.Y('record_count:Q', title='Total Records'),
                color=alt.Color(
                    'Journal:N', 
                    legend=alt.Legend(orient="bottom", direction="horizontal"), 
                    scale=alt.Scale(
                        domain=source_domain, 
                        range=source_range_
                    )
                ),
                tooltip=[
                    alt.Tooltip('journal:N', title='Journal'),
                    alt.Tooltip('record_count:Q', title='Total Records')
                ]
            )
            .properties(width="container", height=300, title='Records by Journal')
            .configure(
                autosize="fit"
            )
            .interactive()
        )

        # Source bar chart
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
            .configure(
                autosize="fit"
            )
            .interactive()
        )
        
        return {
            "year_source": chart_year_source.to_dict(), 
            "country": chart_country.to_dict(), 
            "country_map": country_map.to_dict(),
            "journal": chart_journal.to_dict(), 
            "source": chart_source.to_dict()
        }
        
    def get_summary_statistics(self):
        """
        Fetches summary statistics grouped individually and by Year for each feature: Country, Journal, and Source.
        Returns a dictionary containing grouped data.
        """
        try:
            # Queries for individual grouping and grouping with Year
            queries = {
                # "year_country": """
                #     SELECT "Year", "country", COUNT(*) as record_count
                #     FROM all_db
                #     GROUP BY "Year", "country"
                #     ORDER BY "Year", "country";
                # """,
                # "year_journal": """
                #     SELECT "year", "journal", COUNT(*) as record_count
                #     FROM all_db
                #     GROUP BY "year", "journal"
                #     ORDER BY "year", "journal";
                # """,
                "year_source": """
                    SELECT "year", "source", COUNT(*) as record_count
                    FROM all_db
                    GROUP BY "year", "source"
                    ORDER BY "year", "source";
                """,
                "country": """
                    SELECT "country", COUNT(*) as record_count
                    FROM all_db
                    GROUP BY "country"
                    ORDER BY record_count DESC;
                """,
                "journal": """
                    SELECT "journal", COUNT(*) as record_count
                    FROM all_db
                    GROUP BY "journal"
                    ORDER BY record_count DESC
                    LIMIT 3;
                """,
                "source": """
                    SELECT "source", COUNT(*) AS record_count
                    FROM all_db
                    GROUP BY "source"
                    ORDER BY record_count DESC
                    LIMIT 4;
                """,
                "ovid_grouping": """
                    SELECT "database", COUNT(*) AS record_count
                    FROM all_db
                    WHERE "source"='OVID'
                    GROUP BY "database"
                    ORDER BY record_count DESC
                    LIMIT 4;
                """
            }
            with self.engine.connect() as connection:
                # Execute each query and transform results into a dictionary
                summary_stats = {}
                for key, query in queries.items():
                    result = connection.execute(query).fetchall()
                    summary_stats[key] = [
                        dict(row) for row in result
                    ]

            # print(summary_stats)
            charts = self.generate_chart(summary_stats)
            return {
                "status": "success",
                # "data": summary_stats,
                "charts": charts
            }

        except Exception as e:
            raise Exception(f"Error fetching summary statistics: {e}")