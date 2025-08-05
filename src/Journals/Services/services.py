import json
import logging
from collections import defaultdict
from sqlalchemy.exc import SQLAlchemyError
# Assuming these local imports are correct for your project structure
from src.Commands.regexp import searchRegEx
from src.Utils.Helpers import preprocess_languages
from src.Services.PostgresService import PostgresService
from src.Services.ChartService import ChartService
from src.Journals.Services.APIResponseNormalizer import APIResponseNormalizer


class JSONService:
    """Service for interacting with the database using JSON payloads for CRUD operations."""

    def __init__(self):
        self.db_service = PostgresService()
        self.chart_service = ChartService()

    def _build_from_payload(self, payload):
        """
        Builds a query from a standard JSON payload with full optimization and
        robust, case-insensitive column validation.
        """
        table_name = payload.get("table")
        if not table_name:
            raise ValueError("Table name is required")

        # Initialize the builder and cache the table's schema for validation
        builder = self.db_service.table(table_name)
        valid_columns_lower = builder._table_columns_lower

        # 1. Handle Columns (Silently skips non-existent ones)
        if cols_to_select := payload.get("columns"):
            final_cols = [
                col for col in cols_to_select
                if col == '*' or col.lower() in valid_columns_lower
            ]
            if final_cols:
                builder.select(*final_cols)

        # 2. Handle Filters (Groups conditions and skips non-existent columns)
        filters = payload.get("filters", [])
        if filters:
            grouped_wheres = defaultdict(list)
            grouped_likes = defaultdict(list)
            other_conditions = []

            # First, categorize all filters, skipping any with invalid columns
            for f in filters:
                column = f.get("column")
                # For multi-column filters, all columns must be valid
                if columns := f.get("columns"):
                    if not all(c.lower() in valid_columns_lower for c in columns):
                        logging.warning(
                            f"Filter skipped: One or more columns in {columns} do not exist.")
                        continue
                elif not column or column.lower() not in valid_columns_lower:
                    logging.warning(
                        f"Filter skipped: Column '{column}' does not exist.")
                    continue

                filter_type = f.get("type", "where").lower()
                if filter_type in ["where", "inwhere"]:
                    grouped_wheres[column].append(f.get("value"))
                elif filter_type == "likewhere":
                    grouped_likes[column].append(f.get("value"))
                else:
                    other_conditions.append(f)

            # Apply grouped 'where'/'inwhere' conditions as efficient IN clauses
            for column, values in grouped_wheres.items():
                flat_values = [v for v_list in values for v in (
                    v_list if isinstance(v_list, list) else [v_list])]
                unique_values = list(set(flat_values))
                if len(unique_values) > 1:
                    builder.in_where(column, unique_values)
                elif unique_values:
                    builder.where(column, unique_values[0])

            # Apply grouped 'likewhere' conditions as efficient OR LIKE (...) clauses
            for column, values in grouped_likes.items():
                unique_values = list(set(values))
                if len(unique_values) > 1:
                    builder.or_like_group(column, unique_values)
                elif unique_values:
                    builder.like(column, unique_values[0])

            # Apply all other, non-groupable conditions
            for f in other_conditions:
                op_map = {
                    "orwhere": builder.or_where,
                    "notwhere": lambda col, val: builder.where(col, val, operator="!="),
                    "betweenwhere": builder.between,
                    "orlikewhere": lambda col, val: builder.like(col, val, conjunction="OR"),
                    "multi_column_like": builder.or_like_multi_column,
                }
                filter_type = f.get("type").lower()
                if filter_type in op_map:
                    if filter_type == "multi_column_like":
                        op_map[filter_type](f["columns"], f["value"])
                    elif filter_type == "betweenwhere":
                        op_map[filter_type](
                            f["column"], f["value"][0], f["value"][1])
                    else:
                        op_map[filter_type](f["column"], f["value"])

        # 3. Handle Group By (Case-insensitive check)
        if groups := payload.get("group_by"):
            valid_groups = [col for col in groups if col.lower()
                            in valid_columns_lower]
            if valid_groups:
                builder.group_by(*valid_groups)

        # 4. Handle Aggregations
        for agg in payload.get("aggregations", []):
            col = agg.get("column")
            if col != '*' and (not col or col.lower() not in valid_columns_lower):
                logging.warning(
                    f"Aggregation skipped: Column '{col}' does not exist.")
                continue
            builder.add_aggregation(agg["func"], col, agg.get("alias"))

        # 5. Handle Order By (Case-insensitive check)
        if order := payload.get("order_by"):
            col = order.get("column")
            if col and col.lower() in valid_columns_lower:
                builder.order_by(col, order.get("direction", "ASC"))

        # 6. Handle Pagination
        if page_info := payload.get("pagination"):
            builder.paginate(page_info["page"], page_info["page_size"])

        return builder

    def get_all_filter_options(self, include=None):
        """
        Generates and returns the complete, structured set of all available
        filter options for the UI.
        """
        try:
            # Get all raw data from the database
            all_columns = self.get_columns_from_table({"table": "all_db"})
            tag_filter_data = self.process_columns_with_hash(
                all_columns, searchRegEx)
            regions, countries, languages, years = self.get_other_filters()

            # Assemble the final, structured data object
            countries_manual = [
                "Kyrgyztan", "Bangladesh", "Indonesia", "Italy", "Venezuela", "Oman", "Czech Republic",
                "Sweden", "United Kingdom", "Uganda", "Ireland", "Germany", "Singapore", "Canada",
                "Finland", "Portugal", "South Korea", "Colombia", "Saudi Arabia", "Argentina", "Cuba",
                "England", "Slovenia", "Greece", "Egypt", "Puerto Rico", "Iran, Islamic Republic of",
                "India", "Iran", "Chile", "France", "Estonia", "Vietnam", "Slovakia", "Israel",
                "South Africa", "Peru", "Kenya", "Ghana", "Malaysia", "Hong Kong", "Japan", "Denmark",
                "Bosnia and Herzegovina", "Philippines", "United States", "Turkey", "Nigeria",
                "Switzerland", "New Zealand", "Hungary", "China", "Norway", "Qatar",
                "Scotland", "Pakistan", "Russian Federation", "Netherlands", "Romania", "Brazil",
                "Austria", "Australia", "Serbia", "Ethiopia", "Russia (Federation)", "Bulgaria",
                "Spain", "Croatia", "Libyan Arab Jamahiriya", "Tunisia", "United Arab Emirates",
                "North Macedonia", "Belgium", "Korea (South)", "Mexico", "Nepal", "Tanzania",
                "Poland", "Lebanon", "Taiwan (Republic of China)", "Thailand", "Czechia"
            ]
            # This is the same logic from your FilterAPI
            data = {
                "tag_filters": tag_filter_data.get("data", {}),
                "others": {
                    "Language": sorted(set(languages + ["English"])),
                    "Country": sorted(set(countries + countries_manual)),
                    "Region": sorted(set(regions + ["Americas", "Europe", "Africa"])),
                    "Year": sorted(set([int(float(y)) for y in years] + [2025, 2024, 2023]), reverse=True),
                    "AMSTAR 2 Rating": ["High", "Moderate", "Low", "Critically Low"],
                },
            }
            # Step 2: If no specific filters are requested, return everything.
            if not include:
                return {"success": True, "data": data}

            # Step 3: If specific filters are requested, build a new filtered response.
            include_set = {item.lower() for item in include}
            filtered_data = {}
            
            # Check both 'others' and 'tag_filters' for matches
            all_categories = {**data.get("others", {}), **data.get("tag_filters", {})}          
            for key, value in all_categories.items():
                if key.lower() in include_set:
                    filtered_data[key] = value

            return {"success": True, "data": filtered_data}
        except Exception as e:
            return {"error": str(e)}

    ##################################### START OF TAG COUNT ################################

    def get_contextual_filter_counts(self, payload):
        """
        Calculates the count of records for each filter category based on the
        currently active filters in the payload.
        """
        try:
            # Step 1: Get the complete filter configuration.
            all_filters_response = self.get_all_filter_options()
            if not all_filters_response.get("success"):
                return all_filters_response  # Propagate any errors
            all_filters_config = all_filters_response.get("data", {})

            # Step 2: Create a simple mapping of display names to database columns.
            filter_categories = self._generate_filter_category_mappings(
                all_filters_config)

            # Step 3: Get the base query for the user's current search filters.
            builder = self._build_from_payload(payload)
            base_query_info = builder.show_sql()
            base_query_sql = base_query_info['query']
            base_query_params = base_query_info['params']

            # Isolate the WHERE clause from the base query.
            where_clause = ""
            if ' WHERE ' in base_query_sql:
                # Extract everything from WHERE to the end, then remove ordering/pagination.
                where_clause = " WHERE " + \
                    base_query_sql.split(" WHERE ", 1)[1]
                where_clause = where_clause.split(
                    " ORDER BY ")[0].split(" LIMIT ")[0]

            # Step 4: Build the final aggregation query.
            count_subqueries = []
            for display_name, db_column in filter_categories.items():
                if db_column.lower() in builder._table_columns_lower:

                    # This is the subquery being built
                    subquery = f"""
                        SELECT
                            '{display_name}' as category,
                            CAST("{db_column}" AS TEXT) as value, -- ✅ FIX IS HERE
                            COUNT(*) as count
                        FROM base_results
                        WHERE "{db_column}" IS NOT NULL
                        GROUP BY "{db_column}"
                    """
                    count_subqueries.append(subquery)

            if not count_subqueries:
                # Return empty if no valid categories to count
                return {"success": True, "data": {}}

            # The final query uses a Common Table Expression (CTE) for efficiency.
            final_sql = f"""
                WITH base_results AS (
                    SELECT * FROM {builder._table}
                    {where_clause}
                )
                {' UNION ALL '.join(count_subqueries)};
            """

            # Step 5: Execute the query and format the results.
            raw_counts = self.db_service.execute_raw_query(
                final_sql, base_query_params)

            # Convert the flat list of counts into a nested dictionary.
            formatted_counts = defaultdict(list)
            for row in raw_counts:
                formatted_counts[row['category']].append({
                    "value": row['value'],
                    "count": row['count']
                })

            return {"success": True, "data": dict(formatted_counts)}

        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"error": str(e)}

    def _generate_filter_category_mappings(self, all_filters_config):
        """
        Helper function to dynamically create a mapping of display names
        to database columns from the full filter configuration object.
        """
        category_mappings = {}

        # Process simple categories like 'Year', 'Country' from the 'others' key.
        if 'others' in all_filters_config:
            for category_name in all_filters_config['others']:
                # For simple filters, the display name and column name are the same.
                category_mappings[category_name] = category_name.lower()

        # Process complex, nested 'tag_filters'.
        if 'tag_filters' in all_filters_config:
            for top_level_key, subgroups in all_filters_config['tag_filters'].items():
                for subgroup_key, items in subgroups.items():
                    for item_key, details in items.items():
                        # The display name from the config becomes the key.
                        display_name = details.get('display', item_key)
                        # The database column is constructed from the keys.
                        column_name = f"{top_level_key}__HASH__{subgroup_key}__HASH__{item_key}"
                        category_mappings[display_name] = column_name.lower()

        return category_mappings

    ##################################### END OF TAG COUNT ################################

    # --- CRUD Methods ---

    def create(self, payload):
        try:
            table_name, data = payload.get("table"), payload.get("data")
            if not table_name or not data:
                return {"error": "Table and data are required"}
            # This correctly delegates the work
            self.db_service.table(table_name).add_record(data)
            return {"success": True, "message": "Record created successfully"}
        except Exception as e:
            return {"error": str(e)}

    def search_by_id(self, payload):
        try:
            table_name, record_id = payload.get("table"), payload.get("id")
            if not table_name or not record_id:
                return {"error": "Table and ID are required"}
            record = self.db_service.table(table_name).where(
                "primary_id", record_id).first()
            return {"success": True, "data": record} if record else {"success": False, "message": "Record not found"}
        except Exception as e:
            print(e)
            return {"error": str(e)}

    def search_by_ids(self, id_list, table="all_db"):
        """
        Retrieves a list of records based on a list of primary IDs.
        """
        try:
            if not isinstance(id_list, list) or not id_list:
                return {"success": True, "data": []}

            # Use the builder's 'in_where' for an efficient query
            records = self.db_service.table(
                table).in_where("primary_id", id_list).get()

            return {"success": True, "data": records}
        except Exception as e:
            return {"error": str(e)}

    def read(self, payload):
        try:
            builder = self._build_from_payload(payload)
            return {"success": True, "data": builder.get()}
        except Exception as e:
            return {"error": str(e)}

    def update(self, payload):
        try:
            table, record_id, data = payload.get(
                "table"), payload.get("record_id"), payload.get("data")
            if not all([table, record_id, data]):
                return {"error": "Table, record ID, and data are required"}
            self.db_service.table(table).update_record(record_id, data)
            return {"success": True, "message": "Record updated successfully"}
        except Exception as e:
            return {"error": str(e)}

    def delete(self, payload):
        try:
            table, record_id = payload.get("table"), payload.get("record_id")
            if not all([table, record_id]):
                return {"error": "Table and record ID are required"}
            self.db_service.table(table).delete_record(record_id)
            return {"success": True, "message": "Record deleted successfully"}
        except Exception as e:
            return {"error": str(e)}

    def read_with_raw_input(self, raw_input=None, table="all_db", columns="*", pagination=None, order_by=None, additional_conditions=None, return_sql=False):
        """
        Builds a query from various arguments by converting them into a standard payload,
        then executes the query and performs all necessary post-processing.
        """
        try:
            # 1. Assemble the standard payload from the method's arguments.
            payload = {
                "table": table,
                "filters": []
            }
            if columns != "*":
                payload["columns"] = columns
            if pagination:
                payload["pagination"] = pagination
            if order_by:
                payload["order_by"] = {
                    "column": order_by[0], "direction": order_by[1]}

            # 2. Convert the `raw_input` dict into the standard filter format.
            for col, vals in (raw_input or {}).items():
                if len(vals) > 1:
                    payload["filters"].append(
                        {"type": "inwhere", "column": col, "value": vals})
                else:
                    payload["filters"].append(
                        {"type": "where", "column": col, "value": vals[0]})

            # 3. Add any other conditions to the filters list.
            if additional_conditions:
                payload["filters"].extend(additional_conditions)

            # 4. Use the single, central helper to build the query.
            builder = self._build_from_payload(payload)

            # 5. Handle the option to return the SQL query for debugging.
            # print(builder.show_sql())
            if return_sql:
                return {"success": True, "sql": builder.show_sql()}

            # 6. Execute the query.
            result = builder.get()
            # Handles both paginated and non-paginated results.
            records = result.get('records', result)

            # 7. Perform post-processing to create the notes fields.
            fields_to_extract = [
                "topic__HASH__acceptance__HASH__kaa", "topic__HASH__adm__HASH__adm", "topic__HASH__coverage__HASH__cov", "topic__HASH__eco__HASH__eco",
                "topic__HASH__ethical__issues__HASH__eth", "topic__HASH__modeling__HASH__mod", "topic__HASH__risk__factor__HASH__rf", "topic__HASH__safety__HASH__saf",
                "intervention__HASH__vaccine__options__HASH__adjuvants", "intervention__HASH__vaccine__options__HASH__biva", "intervention__HASH__vaccine__options__HASH__live",
                "intervention__HASH__vaccine__options__HASH__quad", "intervention__HASH__vpd__HASH__diph", "intervention__HASH__vpd__HASH__hb", "intervention__HASH__vpd__HASH__hiv",
                "intervention__HASH__vpd__HASH__hpv", "intervention__HASH__vpd__HASH__infl", "intervention__HASH__vpd__HASH__meas", "intervention__HASH__vpd__HASH__meni",
                "intervention__HASH__vpd__HASH__tetanus", "popu__HASH__age__group__HASH__ado_10__17", "popu__HASH__age__group__HASH__adu_18__64",
                "popu__HASH__age__group__HASH__chi_2__9", "popu__HASH__age__group__HASH__eld_65__10000", "popu__HASH__age__group__HASH__nb_0__1", "popu__HASH__immune__status__HASH__hty",
                "popu__HASH__specific__group__HASH__hcw", "popu__HASH__specific__group__HASH__pcg", "popu__HASH__specific__group__HASH__pw"
            ]
            group_1_fields = [f for f in fields_to_extract if f.startswith(
                "intervention__HASH__vpd__HASH") or f.startswith("topic__HASH__coverage__HASH")]
            group_2_fields = [
                f for f in fields_to_extract if f not in group_1_fields]

            for record in records:
                if not record:
                    continue

                def extract_values(fields):
                    values = []
                    for field in fields:
                        if field in record and record[field]:
                            # Ensure value is treated as a string before splitting
                            values.extend([str(v).split(
                                ":")[-1].strip() for v in str(record[field]).split(",") if ":" in str(v)])
                    return list(filter(None, set(values)))

                record["research_notes"] = ", ".join(
                    extract_values(group_1_fields))
                record["notes"] = ", ".join(extract_values(group_2_fields))

            # 8. Return the final, processed data.
            return {"success": True, "data": result}

        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"error": str(e)}

    # --- Utility and Business Logic Methods ---
    def show_sql(self, payload):
        try:
            builder = self._build_from_payload(payload)
            return {"success": True, "query": builder.show_sql()}
        except Exception as e:
            return {"error": str(e)}

    def get_columns_from_table(self, payload):
        table_name = payload.get("table")
        if not table_name:
            return {"error": "Table name is required"}
        return self.db_service.get_column_names(table_name)

    def process_columns_with_hash(self, filtered_columns, searchRegEx):
        # This method contains business logic and remains unchanged internally.
        structured_data = defaultdict(lambda: defaultdict(dict))
        for column in filtered_columns:
            parts = column.split("__HASH__")
            if len(parts) == 3:
                category, subgroup, value = parts
                corrected_value = value
                if category in searchRegEx and subgroup in searchRegEx[category]:
                    for key in searchRegEx[category][subgroup]:
                        if key.lower().startswith(value.lower()):
                            corrected_value = key
                            break
                synonyms = []
                if category in searchRegEx and subgroup in searchRegEx[category]:
                    tuple_vals = searchRegEx[category][subgroup].get(
                        corrected_value, [])
                    synonyms = [f"{item[0]}:{item[1]}" for item in tuple_vals if isinstance(
                        item, tuple) and len(item) == 2]
                if corrected_value not in synonyms:
                    synonyms.append(corrected_value)
                structured_data[category][subgroup][corrected_value] = {
                    "display": corrected_value,
                    "synonyms": synonyms,
                    "additional_context": None
                }
        data = {key: dict(value) for key, value in structured_data.items()}
        normalizer = APIResponseNormalizer()
        normalized_response = normalizer.normalize_response(data)
        return {"success": True, "data": normalized_response}

    def map_user_selection_to_column(self, user_selections):
        # This orchestrator method remains unchanged internally.
        filtered_columns = self.get_columns_from_table({"table": "all_db"})
        structured_data = self.process_columns_with_hash(
            filtered_columns, searchRegEx).get("data", {})
        result = {}
        for user_selection in user_selections:
            user_selection_lower = user_selection.lower()
            matched = False
            for category, subgroups in structured_data.items():
                for subgroup, values in subgroups.items():
                    for value, details in values.items():
                        if user_selection_lower == details["display"].lower() or user_selection_lower in [syn.lower() for syn in details["synonyms"]]:
                            constructed_column = f"{category}__HASH__{subgroup}__HASH__{value}"[
                                :63]
                            if constructed_column in filtered_columns:
                                if constructed_column not in result:
                                    result[constructed_column] = []
                                result[constructed_column].append(
                                    user_selection)
                                matched = True
                                break
                    if matched:
                        break
                if matched:
                    break
        return {"success": True, "data": result}

    def get_summary_statistics(self):
        # Assumes get_summary_statistics is part of your PostgresService
        # If not, you would build the queries here using the builder.
        if hasattr(self.chart_service, 'get_summary_statistics'):
            return self.chart_service.get_summary_statistics()
        return {"error": "get_summary_statistics not implemented in service"}

    def get_other_filters(self):
        """
        Fetches distinct values for filter dropdowns, removing any None/NULL values
        and gracefully handling cases where a table might not exist.
        """
        unique_region, unique_country = [], []  # Default to empty lists

        try:
            # Attempt to get data from the 'region_country' table
            regions_raw = self.db_service.get_unique_items_from_column(
                "region_country", "region")
            countries_raw = self.db_service.get_unique_items_from_column(
                "region_country", "country")
            # Filter out None values
            unique_region = [r for r in regions_raw if r is not None]
            unique_country = [c for c in countries_raw if c is not None]
        except Exception as e:
            # If an error occurs (e.g., table not found), log it and continue
            logging.warning(f"Could not fetch region/country filters: {e}")
            # unique_region and unique_country will remain as empty lists

        # These calls will run regardless of the try block's success
        languages_raw = self.db_service.get_unique_items_from_column(
            "all_db", "language")
        years_raw = self.db_service.get_unique_items_from_column(
            "all_db", "year")

        # Filter out None and perform any additional processing
        unique_languages = [lang for lang in languages_raw if lang is not None]
        unique_year = [y for y in years_raw if y is not None]
        processed_languages = preprocess_languages(unique_languages)
        # print(unique_region, unique_country, processed_languages, unique_year)
        return unique_region, unique_country, processed_languages, unique_year
