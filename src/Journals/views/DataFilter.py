from typing import Dict
from flask import current_app, request, send_file
from flask_restful import Resource
from src.Journals.Services.ResourceService import ResourceService
from src.Journals.views.DynamicResourceAPI import DynamicResourceAPI
from src.Utils.response import ApiResponse
import json


class ColumnInfoAPI(Resource):
    """Get info about all columns in table"""
    
    def get(self, table_name: str):
        """Get column metadata"""
        try:
            registry = current_app.config['MODEL_REGISTRY']
            model_class = registry.get_by_table_or_fail(table_name)
            
            from sqlalchemy import inspect
            mapper = inspect(model_class)
            
            columns = {}
            for col in mapper.columns:
                columns[col.name] = {
                    'type': str(col.type),
                    'nullable': col.nullable,
                    'primary_key': col.primary_key,
                    'length': col.type.length if hasattr(col.type, 'length') else None
                }
            
            return ApiResponse.success(
                data={
                    'table': table_name,
                    'columns': columns,
                    'total_columns': len(columns)
                },
                message='Column information'
            )
        
        except ValueError as e:
            return ApiResponse.error(
                message=f"Table '{table_name}' not found",
                errors=str(e),
                status_code=404
            )
        except Exception as e:
            return ApiResponse.error(
                message=f"Failed to get column info",
                errors=str(e)
            )




         
# ============================================================================
# SPECIAL ENDPOINTS (DYNAMIC)
# ============================================================================

class FilterAPI_(Resource):
    """Dynamic filters endpoint"""
    
    def get(self, table_name: str):
        """GET endpoint for filter options"""
        try:
            registry = current_app.config['MODEL_REGISTRY']
            model_class = registry.get_by_table_or_fail(table_name)
            repository = registry.get_repository(table_name)
            service = ResourceService(repository, model_class)
            
            # Try to get filter fields from app config
            filter_fields_config = current_app.config.get('FILTER_FIELDS', {})
            filter_fields = filter_fields_config.get(table_name, {})
            
            # If no config, get all columns
            if not filter_fields:
                from sqlalchemy import inspect
                mapper = inspect(model_class)
                filter_fields = {col.name: col.name for col in mapper.columns}
            
            response = service.get_filters(filter_fields)
            
            if response['success']:
                return ApiResponse.success(
                    data=response['data'],
                    message=response['message']
                )
            else:
                return ApiResponse.error(
                    message=response['message'],
                    errors=response.get('error')
                )
        
        except Exception as e:
            return ApiResponse.error(
                message=f"Failed to retrieve filters for {table_name}",
                errors=str(e)
            )


class StatsAPI(Resource):
    """Dynamic statistics endpoint"""
    
    def get(self, table_name: str):
        """GET endpoint for statistics"""
        try:
            registry = current_app.config['MODEL_REGISTRY']
            model_class = registry.get_by_table_or_fail(table_name)
            repository = registry.get_repository(table_name)
            service = ResourceService(repository, model_class)
            
            # Try to get numeric fields from app config
            numeric_fields_config = current_app.config.get('NUMERIC_FIELDS', {})
            numeric_fields = numeric_fields_config.get(table_name, [])
            
            # If no config, find numeric columns automatically
            if not numeric_fields:
                from sqlalchemy import inspect
                from sqlalchemy import Integer, Float, Numeric
                mapper = inspect(model_class)
                numeric_fields = [
                    col.name for col in mapper.columns 
                    if isinstance(col.type, (Integer, Float, Numeric))
                ]
            
            response = service.get_stats(numeric_fields)
            
            if response['success']:
                return ApiResponse.success(
                    data=response['data'],
                    message=response['message']
                )
            else:
                return ApiResponse.error(
                    message=response['message'],
                    errors=response.get('error')
                )
        
        except Exception as e:
            return ApiResponse.error(
                message=f"Failed to retrieve statistics for {table_name}",
                errors=str(e)
            )


class CountAPI(Resource):
    """Dynamic count endpoint"""
    
    def get(self, table_name: str):
        """GET endpoint for count"""
        try:
            registry = current_app.config['MODEL_REGISTRY']
            model_class = registry.get_by_table_or_fail(table_name)
            repository = registry.get_repository(table_name)
            service = ResourceService(repository, model_class)
            
            filters_str = request.args.get('filters', '{}')
            
            try:
                filters = json.loads(filters_str)
            except:
                filters = {}
            
            response = service.count(**filters)
            
            if response['success']:
                return ApiResponse.success(
                    data=response['data'],
                    message=response['message']
                )
            else:
                return ApiResponse.error(
                    message=response['message'],
                    errors=response.get('error')
                )
        
        except Exception as e:
            return ApiResponse.error(
                message=f"Failed to count records in {table_name}",
                errors=str(e)
            )