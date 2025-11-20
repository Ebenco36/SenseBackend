from flask import request
from flask_restful import Resource
from src.Journals.Services.visualization_service import VisualizationService
from src.Utils.response import ApiResponse
# Assuming you have a standard API response formatter
# from src.Utils.ApiResponse import ApiResponse

# Instantiate the service
visualization_service = VisualizationService()

class VisualizationFiltersAPI(Resource):
    def get(self):
        """Endpoint to populate the filter controls."""
        try:
            data = visualization_service.get_filter_data()
            return ApiResponse.success(data=data, message="Filter data retrieved successfully.")
        except Exception as e:
            return ApiResponse.error(message="Failed to retrieve filter data.", errors=str(e))

class VisualizationDataAPI(Resource):
    def get(self):
        """Endpoint to get all aggregated data for the charts."""
        try:
            # Parse and validate query parameters
            start_year = request.args.get('startYear', type=int)
            end_year = request.args.get('endYear', type=int)
            topic = request.args.get('topic') # Can be None if not provided
            # Call the service to get the aggregated data
            data = visualization_service.get_dashboard_data(start_year, end_year, topic)
            
            return ApiResponse.success(data=data, message="Dashboard data retrieved successfully.")
        except Exception as e:
            return ApiResponse.error(message="Failed to retrieve dashboard data.", errors=str(e))