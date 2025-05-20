import json
import pandas as pd
from io import BytesIO, StringIO
from flask_restful import Resource
from src.Utils.response import ApiResponse
from src.Commands.regexp import searchRegEx
from src.Journals.Services.services import JSONService
from flask import request, jsonify, send_file, make_response

# Initialize JSONService
json_service = JSONService()


class FilterAPI(Resource):
    def get(self):
        try:
            # Default query parameters
            query_params = {"table": "all_db"}
            # Use JSON payload if available, otherwise fallback to default query params
            payload = request.get_json() if request.is_json else query_params

            # Process the request using the service layer
            columns = json_service.get_columns_from_table(payload)
            
            response = json_service.process_columns_with_hash(columns, searchRegEx)

            regions, countries, languages, years = json_service.get_other_filters()

            data = {
                "tag_filters": response.get("data", {}),
                "others": {
                    "Language": sorted(
                        set(
                            # languages + [ 
                            #     "Arabic", "Bosnian", "Bulgarian", "Chinese", "Croatian", "Czech", 
                            #     "Danish", "Dutch", "English", "French", "German", "Greek", "Hebrew"
                            #     "Italian", "Japanese", "Korean", "Norwegian", "Persian", "Polish"
                            #     "Portuguese", "Russian", "Spanish", "Turkish"
                            # ]
                            [ 
                                "English"
                                
                            ]
                        )
                    ),  #  +
                    "Country": sorted(
                        set(
                            countries
                            + [
                                "Germany", "France", "Turkey", "China",
                                "Brasil", "Canada", "Nigeria",
                            ]
                        )
                    ),
                    "region": sorted(
                        set(regions + [
                                "Americas", "Europe", "Africa", "Asia", 
                                "North America", "South America",
                                "Oceania", "Unknown"
                            ]
                        )
                    ),
                    "Year": sorted(
                        set(
                            [int(float(year)) for year in years] + [
                                2025, 2024, 2023, 2022, 2021, 2020, 
                                2019, 2018, 2017, 2016, 2015, 2014, 
                                2013, 2012, 2011
                            ]
                        ),
                        reverse=True,
                    ),  # ,
                    "AMSTAR 2 Rating": [
                        "High",
                        "Moderate",
                        "Low",
                        "Critically Low"
                    ],
                },
            }
            # Check for success in the response
            if "success" in response:
                return ApiResponse.success(
                    data=data, message="Columns processed successfully", status_code=200
                )
            else:
                return ApiResponse.error(
                    errors=response.get("error", "Unknown error occurred"),
                    status_code=400,
                )
        except Exception as e:
            # Handle unexpected exceptions
            return ApiResponse.error(
                message="An unexpected error occurred", errors=str(e), status_code=500
            )


class FetchRecordAPI(Resource):
    def get(self, id):
        try:
            # Default query parameters
            query_params = {"table": "all_db", "id": id}
            # Process the request using the service layer
            response = json_service.search_by_id(query_params)
            if response.get("success") is True and response.get("data") is not None:
                # Process the response data
                record = response["data"]
                # Full list of fields to extract
                fields_to_extract = [
                    "topic__HASH__acceptance__HASH__kaa", "topic__HASH__adm__HASH__adm",
                    "topic__HASH__coverage__HASH__cov", "topic__HASH__eco__HASH__eco",
                    "topic__HASH__ethical__issues__HASH__eth", "topic__HASH__modeling__HASH__mod",
                    "topic__HASH__risk__factor__HASH__rf", "topic__HASH__safety__HASH__saf",
                    "intervention__HASH__vaccine__options__HASH__adjuvants", "intervention__HASH__vaccine__options__HASH__biva",
                    "intervention__HASH__vaccine__options__HASH__live", "intervention__HASH__vaccine__options__HASH__quad",
                    "intervention__HASH__vpd__HASH__diph", "intervention__HASH__vpd__HASH__hb",
                    "intervention__HASH__vpd__HASH__hiv", "intervention__HASH__vpd__HASH__hpv",
                    "intervention__HASH__vpd__HASH__infl", "intervention__HASH__vpd__HASH__meas",
                    "intervention__HASH__vpd__HASH__meni", "intervention__HASH__vpd__HASH__tetanus",
                    "popu__HASH__age__group__HASH__ado_10__17", "popu__HASH__age__group__HASH__adu_18__64",
                    "popu__HASH__age__group__HASH__chi_2__9", "popu__HASH__age__group__HASH__eld_65__10000",
                    "popu__HASH__age__group__HASH__nb_0__1", "popu__HASH__immune__status__HASH__hty",
                    "popu__HASH__specific__group__HASH__hcw", "popu__HASH__specific__group__HASH__pcg",
                    "popu__HASH__specific__group__HASH__pw"
                ]
                # Dynamically categorize fields into two groups
                group_1_fields = [field for field in fields_to_extract if field.startswith("intervention__HASH__vpd__HASH") or field.startswith("topic__HASH__coverage__HASH")]
                group_2_fields = [field for field in fields_to_extract if field not in group_1_fields]
                # Use the helper for both groups
                extracted_group_1_values = json_service.extract_group_values(record, group_1_fields)
                extracted_group_2_values = json_service.extract_group_values(record, group_2_fields)

                extracted_group_1_values = list(filter(None, set(extracted_group_1_values)))
                extracted_group_2_values = list(filter(None, set(extracted_group_2_values)))
                # print(extracted_group_2_values)
                # Add new fields
                record["research_notes"] = ", ".join(extracted_group_1_values)  # Notes for Group 1
                record["notes"] = ", ".join(extracted_group_2_values)  # Notes for Group 2
            # update data in response
            response["data"] = record
            # Check for success in the response
            if "success" in response:
                return ApiResponse.success(
                    data=response.get("data", {}),
                    message="Record fetched sucessfully.",
                    status_code=200,
                )
            else:
                return ApiResponse.error(
                    errors=response.get("error", "Unknown error occurred"),
                    status_code=400,
                )
        except Exception as e:
            # Handle unexpected exceptions
            return ApiResponse.error(
                message="An unexpected error occurred", errors=str(e), status_code=500
            )


class ProcessUserSelectionAPI(Resource):
    def post(self):
        """
        Example Payload:
        {
            "user_selections": ["death", "mortality", "caregivers", "Awareness", "knowledge"],
            "columns": ["Id", "Title"],
            "additional_fields": [
                {
                    "column": "Title",
                    "value": "Perceptions and Experiences of ",
                    "type": "orlikewhere"
                },
                {
                    "column": "Authors",
                    "value": "Hannah",
                    "type": "likewhere"
                }
            ],
            "pagination": {"page": 1, "page_size": 10},
            "order_by": ["primary_id", "ASC"],
            "export": "csv"  # Optional: csv, excel, or json
        }
        """
        try:
            # Parse JSON payload
            payloads = request.get_json()
            if not payloads:
                return ApiResponse.error(
                    message="Invalid payload: Missing or malformed JSON",
                    status_code=400,
                )

            # Extract data from the payload
            user_selection = payloads.get("user_selections", [])
            other_user_selection = payloads.get("others", [])
            columns = payloads.get("columns", "*")
            additional_fields = payloads.get("additional_fields", [])
            pagination = payloads.get("pagination", {"page": 1, "page_size": 10})
            order_by = tuple(payloads.get("order_by", ["primary_id", "ASC"]))
            additional_conditions = additional_fields
            export_format = payloads.get("export", None)

            # Process user selection
            processed_data = json_service.map_user_selection_to_column(user_selection)
            raw_input = processed_data.get("data", {})

            # Call service to read data with raw input and additional parameters
            response = json_service.read_with_raw_input(
                raw_input,
                columns=columns,
                pagination=pagination,
                order_by=order_by,
                additional_conditions=additional_conditions,
                return_sql=False,
            )

            # Check for success in the service response
            if "success" not in response or not response["success"]:
                return ApiResponse.error(
                    errors=response.get("error", "Unknown error occurred"),
                    status_code=400,
                )

            # Handle export functionality
            if export_format:
                return self.export_response(response["data"], export_format)

            # Return JSON response if no export is requested
            return ApiResponse.success(
                data=response, message="Data processed successfully", status_code=200
            )

        except Exception as e:
            # Log traceback for debugging
            import traceback

            traceback.print_exc()
            return ApiResponse.error(
                message="An unexpected error occurred", errors=str(e), status_code=500
            )

    @staticmethod
    def export_response(data, export_format):
        """
        Export the response data in the desired format.
        :param data: The response data to export.
        :param export_format: The desired export format (csv, excel, json).
        :return: Flask response containing the exported file.
        """
        if not data:
            return ApiResponse.error(
                message="No data available to export.", status_code=400
            )

        # Convert data to a DataFrame for export
        df = pd.DataFrame(data)
        df = df[[
            "Authors",
            "Title",
            "Authors",
            "DOI",
            "doi_url",
            "Source",
            "Year",
            "Language",
            "Country",
            "region",
            "open_acc__HASH__opn_access__HASH__op_ac",
            "Abstract",
            "lit_search_dates__HASH__dates__HASH__dates", "num_databases", "database_list",
            "dosage", "comparator", "topic__HASH__eff__HASH__eff",
            "amstar_label", "amstar_flaws", "research_notes", "notes"
        ]]

        # 2. Rename columns via a dict
        df = df.rename(columns={
            "open_acc__HASH__opn_access__HASH__op_ac": "Open_Access",
            "lit_search_dates__HASH__dates__HASH__dates": "Search Dates",
            "topic__HASH__eff__HASH__eff": "Effectiveness & Efficacy"
        })

        # Handle CSV export
        if export_format == "csv":
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            response = make_response(csv_buffer.getvalue())
            response.headers["Content-Disposition"] = "attachment; filename=data.csv"
            response.headers["Content-Type"] = "text/csv"
            return response

        # Handle Excel export
        elif export_format == "excel":
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name="Sheet1")
            excel_buffer.seek(0)
            return send_file(
                excel_buffer,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name="data.xlsx",
            )

        # Handle JSON export
        elif export_format == "json":
            response = make_response(json.dumps(data, indent=4))
            response.headers["Content-Disposition"] = "attachment; filename=data.json"
            response.headers["Content-Type"] = "application/json"
            return response

        # Unsupported format
        return ApiResponse.error(
            message=f"Export format '{export_format}' is not supported.",
            status_code=400,
        )


class SummaryStatisticsAPI(Resource):
    """
    Class-based API for fetching summary statistics using a service.
    """

    def get(self):
        """
        Handles GET requests to fetch summary statistics.
        """
        try:
            # Use the service to get summary statistics
            summary_stats = json_service.get_summary_statistics()
            return ApiResponse.success(
                data=summary_stats, message="success", status_code=200
            )
        except Exception as e:
            return ApiResponse.error(
                message="An unexpected error occurred", errors=str(e), status_code=500
            )
