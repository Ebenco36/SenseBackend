# src/Middleware/record_processor_middleware.py
"""
Middleware to automatically add artificial columns to all API responses.
"""
from flask import request, jsonify
from src.Services.DBservices.RecordProcessor import RecordProcessor
import logging

logger = logging.getLogger(__name__)


class RecordProcessorMiddleware:
    """Automatically processes records in API responses."""
    
    def __init__(self, app=None):
        self.processor = RecordProcessor(include_empty=False)
        if app:
            self.init_app(app)
    
    def init_app(self, app):
        """Initialize middleware with Flask app."""
        app.after_request(self.process_response)
    
    def process_response(self, response):
        """Process response and add artificial columns."""
        try:
            if response.status_code == 200 and response.is_json:
                data = response.get_json()
                
                if data and isinstance(data, dict):
                    processed = self._process_data(data)
                    
                    if processed:
                        response.data = jsonify(data).data
                        response.headers['Content-Type'] = 'application/json'
        
        except Exception as e:
            logger.warning(f"Error in RecordProcessorMiddleware: {e}")
        
        return response
    
    def _process_data(self, data: dict) -> bool:
        """Process data and add artificial columns."""
        modified = False
        
        if 'data' in data:
            # Single record
            if isinstance(data['data'], dict) and 'primary_id' in data['data']:
                records = self.processor.add_artificial_columns([data['data']])
                data['data'] = self._format_record(records[0])
                modified = True
            
            # List of records
            elif isinstance(data['data'], list):
                records = self.processor.add_artificial_columns(data['data'])
                data['data'] = [self._format_record(r) for r in records]
                modified = True
            
            # Nested records array
            elif isinstance(data['data'], dict) and 'records' in data['data']:
                if isinstance(data['data']['records'], list):
                    records = self.processor.add_artificial_columns(data['data']['records'])
                    data['data']['records'] = [self._format_record(r) for r in records]
                    modified = True
        
        return modified
    
    def _format_record(self, record: dict) -> dict:
        """
        Format a single record for frontend.
        Converts comma-separated strings to arrays.
        """
        # ✅ Convert artificial columns to arrays
        for field in ['research_notes', 'notes', 'topic_notes']:
            if field in record:
                record[field] = self._split_to_array(record[field])
        
        return record
    
    def _split_to_array(self, value) -> list:
        """Convert comma-separated string to array."""
        if not value:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [v.strip() for v in value.split(',') if v.strip()]
        return []