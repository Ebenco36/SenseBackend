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
            # Just call the new helper method
            response = json_service.get_all_filter_options()

            if response.get("success"):
                return ApiResponse.success(
                    data=response.get("data"),
                    message="Filters retrieved successfully."
                )
            else:
                return ApiResponse.error(errors=response.get("error"))
        except Exception as e:
            return ApiResponse.error(message="An unexpected error occurred", errors=str(e))


class FetchRecordAPI(Resource):
    def get(self, id):
        try:
            # Default query parameters
            query_params = {"table": "all_db", "id": id}
            # Process the request using the service layer
            response = json_service.search_by_id(query_params)
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


class FetchRecordsByIdsAPI(Resource):
    """Resource to fetch multiple records by delegating to the JSONService."""

    def get(self):
        try:
            # 1. Get and parse the IDs from the request (API layer responsibility)
            ids_str = request.args.get('ids')
            if not ids_str:
                return ApiResponse.error(
                    message="The 'ids' query parameter is required.",
                    status_code=400
                )
            try:
                id_list = [int(i.strip())
                           for i in ids_str.split(',') if i.strip()]
            except ValueError:
                return ApiResponse.error(
                    message="Invalid ID format. Please provide only comma-separated integers.",
                    status_code=400
                )
            response = json_service.search_by_ids(id_list)
            if response.get("success"):
                return ApiResponse.success(
                    data=response.get("data", []),
                    message=f"Successfully fetched {len(response.get('data', []))} record(s).",
                    status_code=200
                )
            else:
                return ApiResponse.error(
                    errors=response.get("error", "Unknown error occurred"),
                    status_code=400
                )

        except Exception as e:
            return ApiResponse.error(
                message="An unexpected error occurred",
                errors=str(e),
                status_code=500
            )


class ProcessUserSelectionAPI(Resource):
    def post(self):
        """
        Example Payload:
        {
            "user_selections": ["death", "mortality", "caregivers", "Awareness", "knowledge"],
            "columns": ["id", "Title"],
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
            # other_user_selection = payloads.get("others", [])
            columns = payloads.get("columns", "*")
            additional_fields = payloads.get("additional_fields", [])
            pagination = payloads.get(
                "pagination", {"page": 1, "page_size": 10})
            order_by = tuple(payloads.get("order_by", ["primary_id", "ASC"]))
            export_format = payloads.get("export", None)

            order_by_dict = None
            # Ensure the list is valid before converting it to a dictionary
            if isinstance(order_by, list) and len(order_by) == 2:
                order_by_dict = {
                    "column": order_by[0], "direction": order_by[1]}

            # Define which columns a Title search should expand to
            title_search_columns = ["title", "authors", "abstract", "country", "journal"]

            final_conditions = []
            for f in additional_fields:
                # Check for a filter where the column is 'Title'
                if f.get("column") and f.get("column").lower() == "title":
                    # Transform it into our new custom filter type
                    final_conditions.append({
                        "type": "multi_column_like",
                        "columns": title_search_columns,
                        "value": f.get("value")
                    })
                else:
                    # Keep all other filters as they are
                    final_conditions.append(f)

            # Process user selection
            processed_data = json_service.map_user_selection_to_column(
                user_selection)
            raw_input = processed_data.get("data", {})
            # Call service to read data with raw input and additional parameters
            response = json_service.read_with_raw_input(
                raw_input,
                columns=columns,
                pagination=pagination,
                order_by=order_by,
                additional_conditions=final_conditions,
                return_sql=False,
                table="all_db"
            )

            payloads["table"] = "all_db"
            payloads["order_by"] = order_by_dict
            filter_counts_response = json_service.get_contextual_filter_counts(
                payloads)
            response['filter_counts_response'] = filter_counts_response
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
