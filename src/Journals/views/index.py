from flask import request
from flask_restful import Resource
from src.Journals.services import JSONService
from src.Utils.response import ApiResponse

# Initialize JSONService
json_service = JSONService()


class RecordsAPI(Resource):
    """Class-based resource for CRUD operations on records."""

    def post(self):
        """
        Create a new record.
        Example Payload:
        {
            "table": "my_table",
            "data": {"name": "John Doe", "status": "active"}
        }
        """
        try:
            payload = request.get_json()
            if not payload:
                return ApiResponse.error(message="Invalid payload: Missing or malformed JSON", status_code=400)

            response = json_service.create(payload)
            if response.get("success"):
                return ApiResponse.success(
                    message=response.get("message", "Record created successfully"), 
                    status_code=201
                )
            else:
                return ApiResponse.error(
                    errors=response.get("error", "Unknown error occurred"), 
                    status_code=400
                )
        except Exception as e:
            logger.error(f"Error in RecordsAPI.post: {str(e)}")
            return ApiResponse.error(message="An unexpected error occurred", status_code=500)

    def get(self):
        """
        Retrieve records with filters, pagination, and sorting.
        Example Query Parameters or JSON Payload:
        {
            "table": "my_table",
            "columns": ["id", "name"],
            "filters": [{"type": "where", "column": "status", "value": "active"}],
            "order_by": {"column": "id", "direction": "ASC"},
            "pagination": {"page": 1, "page_size": 10}
        }
        """
        try:
            # Determine payload source based on request type
            query_params = request.args.to_dict()
            payload = request.get_json() if request.is_json else query_params

            # Call the service layer to handle the logic
            response = json_service.read(payload)

            # Use ApiResponse to format the response
            if "success" in response:
                return ApiResponse.success(data=response.get("data", {}), message="Operation successful", status_code=200)
            else:
                return ApiResponse.error(errors=response.get("error", "Unknown error"), status_code=400)
        except Exception as e:
            # Handle unexpected exceptions with ApiResponse
            return ApiResponse.error(message="An unexpected error occurred", errors=str(e), status_code=500)

    def put(self, record_id):
        """
        Update an existing record.
        Example Payload:
        {
            "table": "my_table",
            "data": {"status": "inactive"}
        }
        """
        try:
            payload = request.get_json()
            if not payload:
                return ApiResponse.error(message="Invalid payload: Missing or malformed JSON", status_code=400)

            # Add `record_id` to the payload
            payload["record_id"] = record_id

            # Process the update using the service layer
            response = json_service.update(payload)

            # Determine the response based on the service output
            if "success" in response:
                return ApiResponse.success(data=response, message="Record updated successfully", status_code=200)
            else:
                return ApiResponse.error(errors=response.get("error", "Unknown error occurred"), status_code=400)
        except Exception as e:
            # Handle unexpected exceptions
            return ApiResponse.error(message="An unexpected error occurred", errors=str(e), status_code=500)

    def delete(self, record_id):
        """
        Delete a record by ID.
        Example Query Parameter:
            ?table=my_table
        """
        try:
            # Get table name from query parameters
            table_name = request.args.get("table")
            if not table_name:
                return ApiResponse.error(message="Table name is required", status_code=400)

            # Prepare the payload for deletion
            payload = {"table": table_name, "record_id": record_id}

            # Process the delete operation
            response = json_service.delete(payload)

            # Check the response for success or error
            if "success" in response:
                return ApiResponse.success(data=response, message="Record deleted successfully", status_code=200)
            else:
                return ApiResponse.error(errors=response.get("error", "Unknown error occurred"), status_code=400)
        except Exception as e:
            # Handle unexpected exceptions
            return ApiResponse.error(message="An unexpected error occurred", errors=str(e), status_code=500)
