# src/Middleware/record_processor_middleware.py
"""
Middleware to automatically add artificial columns to all API responses.
"""
from flask import request, jsonify
from src.Services.DBservices.RecordProcessor import RecordProcessor
import logging

logger = logging.getLogger(__name__)


class RecordProcessorMiddleware:
    """
    Automatically processes records in API responses.
    Apply this globally to add artificial columns everywhere.
    """
    
    def __init__(self, app=None):
        self.processor = RecordProcessor()
        if app:
            self.init_app(app)
    
    def init_app(self, app):
        """Initialize middleware with Flask app."""
        app.after_request(self.process_response)
    
    def process_response(self, response):
        """
        Process response and add artificial columns if it contains records.
        Only processes JSON responses with 'data' field.
        """
        try:
            # Only process successful JSON responses
            if response.status_code == 200 and response.is_json:
                data = response.get_json()
                
                if data and isinstance(data, dict):
                    # Check if response has records to process
                    processed = self._process_data(data)
                    
                    if processed:
                        response.data = jsonify(data).data
                        response.headers['Content-Type'] = 'application/json'
        
        except Exception as e:
            logger.warning(f"Error in RecordProcessorMiddleware: {e}")
        
        return response
    
    def _process_data(self, data: dict) -> bool:
        """
        Process data structure and add artificial columns.
        Returns True if processing was applied.
        """
        modified = False
        
        # Check for 'data' field with records
        if 'data' in data:
            # Single record
            if isinstance(data['data'], dict) and 'primary_id' in data['data']:
                data['data'] = self.processor.add_artificial_columns([data['data']])[0]
                modified = True
            
            # List of records
            elif isinstance(data['data'], list):
                data['data'] = self.processor.add_artificial_columns(data['data'])
                modified = True
            
            # Nested records array
            elif isinstance(data['data'], dict) and 'records' in data['data']:
                if isinstance(data['data']['records'], list):
                    data['data']['records'] = self.processor.add_artificial_columns(
                        data['data']['records']
                    )
                    modified = True
        
        return modified
