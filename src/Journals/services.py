import json
from sqlalchemy.exc import SQLAlchemyError
from src.Services.PostgresService import PostgresService

class JSONService:
    """Service for interacting with the database using JSON payloads for CRUD operations."""

    def __init__(self):
        self.db_service = PostgresService()

    def create(self, payload):
        """
        Create a new record in the specified table.
        Example Payload:
        {
            "table": "my_table",
            "data": {"name": "John Doe", "status": "active"}
        }
        """
        try:
            table_name = payload.get("table")
            data = payload.get("data")
            if not table_name or not data:
                return {"error": "Table name and data are required"}

            success = self.db_service.table(table_name).add_record(data)
            return {"success": success, "message": "Record created successfully"} if success else {"error": "Failed to create record"}
        except Exception as e:
            return {"error": str(e)}

    def read(self, payload):
        """
        Read records from the specified table with filters, pagination, and column selection.
        Example Payload:
        {
            "table": "my_table",
            "columns": ["id", "name"],
            "filters": [{"type": "where", "column": "status", "value": "active"}],
            "order_by": {"column": "id", "direction": "ASC"},
            "pagination": {"page": 1, "page_size": 10}
        }
        """
        try:
            table_name = payload.get("table")
            if not table_name:
                return {"error": "Table name is required"}

            service = self.db_service.table(table_name)

            # Handle column selection
            columns = payload.get("columns", [])
            if columns:
                service = service.select(*columns)

            # Handle filters
            filters = payload.get("filters", [])
            for filter_obj in filters:
                filter_type = filter_obj["type"]
                column = filter_obj["column"]
                value = filter_obj["value"]
                if filter_type == "where":
                    service = service.where(column, value)
                elif filter_type == "orWhere":
                    service = service.orWhere(column, value)
                elif filter_type == "likeWhere":
                    service = service.likeWhere(column, value)
                elif filter_type == "inWhere":
                    service = service.inWhere(column, value)
                elif filter_type == "betweenWhere":
                    service = service.betweenWhere(column, value[0], value[1])

            # Handle order by
            order_by = payload.get("order_by", {})
            if order_by:
                service = service.orderBy(order_by["column"], order_by.get("direction", "ASC"))

            # Handle pagination
            pagination = payload.get("pagination", {})
            if pagination:
                service = service.paginate(pagination.get("page", 1), pagination.get("page_size", 10))

            # Execute query and return results
            return {"success": True, "data": service.get()}
        except Exception as e:
            return {"error": str(e)}

    def update(self, payload):
        """
        Update an existing record in the specified table.
        Example Payload:
        {
            "table": "my_table",
            "record_id": 1,
            "data": {"status": "inactive"}
        }
        """
        try:
            table_name = payload.get("table")
            record_id = payload.get("record_id")
            data = payload.get("data")
            if not table_name or not record_id or not data:
                return {"error": "Table name, record ID, and data are required"}

            success = self.db_service.table(table_name).update_record(record_id, data)
            return {"success": success, "message": "Record updated successfully"} if success else {"error": "Failed to update record"}
        except Exception as e:
            return {"error": str(e)}

    def delete(self, payload):
        """
        Delete a record from the specified table.
        Example Payload:
        {
            "table": "my_table",
            "record_id": 1
        }
        """
        try:
            table_name = payload.get("table")
            record_id = payload.get("record_id")
            if not table_name or not record_id:
                return {"error": "Table name and record ID are required"}

            success = self.db_service.table(table_name).delete_record(record_id)
            return {"success": success, "message": "Record deleted successfully"} if success else {"error": "Failed to delete record"}
        except Exception as e:
            return {"error": str(e)}
