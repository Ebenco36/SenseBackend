from flask import request, jsonify
from flask_restful import Resource
from src.Journals.services import JSONService

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
            payload = request.json
            if not payload:
                return {"error": "Invalid payload"}, 400

            response = json_service.create(payload)
            return response, 201 if "success" in response else 400
        except Exception as e:
            return {"error": str(e)}, 500

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
            query_params = request.args.to_dict()
            payload = request.json if request.is_json else query_params

            response = json_service.read(payload)
            return response, 200 if "success" in response else 400
        except Exception as e:
            return {"error": str(e)}, 500

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
            payload = request.json
            if not payload:
                return {"error": "Invalid payload"}, 400

            payload["record_id"] = record_id
            response = json_service.update(payload)
            return response, 200 if "success" in response else 400
        except Exception as e:
            return {"error": str(e)}, 500

    def delete(self, record_id):
        """
        Delete a record by ID.
        Example Query Parameter:
            ?table=my_table
        """
        try:
            table_name = request.args.get("table")
            if not table_name:
                return {"error": "Table name is required"}, 400

            payload = {"table": table_name, "record_id": record_id}
            response = json_service.delete(payload)
            return response, 200 if "success" in response else 400
        except Exception as e:
            return {"error": str(e)}, 500
