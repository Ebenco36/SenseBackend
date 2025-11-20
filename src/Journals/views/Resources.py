"""
Filter Resources - Using ApplicationService
"""

from flask_restful import Resource
from flask import current_app
from src.Utils.response import ApiResponse
import logging

logger = logging.getLogger(__name__)


class FiltersTreeResource(Resource):
    """GET /api/v1/filters/tree"""
    
    def get(self):
        """Get all filters using ApplicationService"""
        try:
            app_service = current_app.config.get('APP_SERVICE')
            
            if not app_service:
                logger.error("APP_SERVICE not initialized")
                return ApiResponse.error(
                    message="Application service not initialized",
                    status_code=500
                )
            
            # Use ApplicationService which uses FilterService
            result = app_service.get_filter_tree()
            
            if result.get('success'):
                return ApiResponse.success(
                    data=result.get('data'),
                    message="Filters retrieved successfully"
                )
            else:
                return ApiResponse.error(
                    message=result.get('message', 'Failed to get filters'),
                    errors=result.get('errors'),
                    status_code=500
                )
        
        except Exception as e:
            logger.error(f"Error fetching filters: {str(e)}", exc_info=True)
            
            return ApiResponse.error(
                message="Failed to retrieve filters",
                errors=[str(e)],
                status_code=500
            )
            


"""
filter_resource.py - Enhanced with nested OR condition support
✅ Handles nested logic groups (OR within AND)
✅ Recursive filter application
✅ Multi-field OR search compatible
"""

from flask import request, send_file
from flask_restful import Resource
from sqlalchemy import and_, or_, func, between, inspect as sql_inspect
from typing import Dict, List, Any, Optional
import logging
import io
import csv
import json
from datetime import datetime
from collections import defaultdict

from database.db import db
from src.Utils.filter_structure import FILTER_STRUCTURE

logger = logging.getLogger(__name__)

class FilterSearchResource(Resource):
    """
    Search and filter records with nested OR support.
    
    POST /api/v1/filters/search
    """
    
    def __init__(self):
        """Initialize with no external dependencies."""
        self.logger = logger
    
    def post(self):
        """Search records with filtering, pagination, sorting."""
        try:
            data = request.get_json()
            if not data:
                return {
                    'success': False,
                    'error': 'No JSON data provided',
                    'message': 'Request body must contain JSON'
                }, 400
            
            table_name = data.get('table_name', 'all_db')
            search = data.get('search', {})
            sort_by = data.get('sort_by', 'primary_id')
            sort_direction = data.get('sort_direction', 'asc').lower()
            pagination = data.get('pagination', {'page': 1, 'page_size': 20})
            export_format = data.get('export')
            
            self.logger.info(f"POST /filters/search - table: {table_name}")
            
            # Get model
            model_class = self._get_model(table_name)
            if not model_class:
                return {
                    'success': False,
                    'error': f'Table {table_name} not found',
                    'message': 'Invalid table name'
                }, 404
            
            # Build query
            query = db.session.query(model_class)
            
            # Apply filters (supports nested OR/AND)
            if search and 'conditions' in search:
                query = self._apply_filters_recursive(
                    query, 
                    model_class, 
                    search['conditions'], 
                    search.get('logic', 'AND')
                )
            
            # Get total count
            total_records = query.count()
            
            # Apply sorting
            query = self._apply_sorting(query, model_class, sort_by, sort_direction)
            
            # Handle export
            if export_format:
                return self._handle_export(query, export_format, model_class)
            
            # Apply pagination
            page = pagination.get('page', 1)
            page_size = pagination.get('page_size', 20)
            offset = (page - 1) * page_size
            query = query.limit(page_size).offset(offset)
            
            # Execute query
            records = query.all()
            serialized_records = [self._serialize_record(r) for r in records]
            
            # Pagination info
            total_pages = (total_records + page_size - 1) // page_size
            
            # Filter counts
            filter_counts = self._get_filter_counts(model_class, search)
            
            return {
                'success': True,
                'data': {
                    'records': serialized_records,
                    'pagination': {
                        'current_page': page,
                        'page_size': page_size,
                        'total_pages': total_pages,
                        'total_records': total_records
                    },
                    'filter_counts': filter_counts
                },
                'message': 'Search completed successfully'
            }, 200
        
        except Exception as e:
            self.logger.error(f"Error in POST /filters/search: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'message': 'Search failed'
            }, 500
    
    def _get_model(self, table_name: str):
        """Get model class from SQLAlchemy."""
        try:
            for mapper in db.Model.registry.mappers:
                model = mapper.class_
                if hasattr(model, '__tablename__'):
                    if model.__tablename__ == table_name:
                        return model
                    if model.__tablename__.replace('_', '').lower() == table_name.replace('_', '').lower():
                        return model
            return None
        except Exception as e:
            self.logger.error(f"Error getting model: {e}")
            return None
    
    def _apply_filters_recursive(
        self, 
        query, 
        model_class, 
        conditions: List[Dict], 
        logic: str = 'AND'
    ):
        """
        ✅ ENHANCED: Apply filters with recursive nested OR/AND support.
        
        Handles structures like:
        {
            "logic": "AND",
            "conditions": [
                {
                    "logic": "OR",
                    "conditions": [
                        {"field": "title", "operator": "contains", "value": "home"},
                        {"field": "authors", "operator": "contains", "value": "home"}
                    ]
                },
                {"field": "year", "operator": "gte", "value": 2020}
            ]
        }
        """
        try:
            if not conditions:
                return query
            
            filter_clauses = []
            
            for condition in conditions:
                # Check if this is a nested logic group
                if 'logic' in condition and 'conditions' in condition:
                    # Recursive call for nested group
                    nested_clauses = []
                    for nested_condition in condition['conditions']:
                        clause = self._build_single_clause(model_class, nested_condition)
                        if clause is not None:
                            nested_clauses.append(clause)
                    
                    # Apply nested logic
                    if nested_clauses:
                        nested_logic = condition.get('logic', 'AND').upper()
                        if nested_logic == 'OR':
                            filter_clauses.append(or_(*nested_clauses))
                        else:
                            filter_clauses.append(and_(*nested_clauses))
                
                else:
                    # Simple condition
                    clause = self._build_single_clause(model_class, condition)
                    if clause is not None:
                        filter_clauses.append(clause)
            
            # Apply filters to query
            if filter_clauses:
                if logic.upper() == 'OR':
                    query = query.filter(or_(*filter_clauses))
                else:
                    query = query.filter(and_(*filter_clauses))
            
            return query
        
        except Exception as e:
            self.logger.error(f"Error applying filters: {e}", exc_info=True)
            return query
    
    # def _build_single_clause(self, model_class, condition: Dict):
    #     """Build a single SQLAlchemy filter clause."""
    #     try:
    #         field = condition.get('field')
    #         operator = condition.get('operator', 'contains')
    #         value = condition.get('value')
    #         values = condition.get('values', [])
            
    #         # Validate field exists
    #         if not field or not hasattr(model_class, field):
    #             self.logger.warning(f"Field '{field}' not found in model")
    #             return None
            
    #         column = getattr(model_class, field)
            
    #         # Build clause based on operator
    #         if operator == 'contains':
    #             return column.ilike(f'%{value}%')
    #         elif operator in ['equals', '=']:
    #             return column == value
    #         elif operator == 'in':
    #             if values:
    #                 return column.in_(values)
    #         elif operator == 'between':
    #             if isinstance(value, list) and len(value) == 2:
    #                 return between(column, value[0], value[1])
    #         elif operator == 'starts_with':
    #             return column.ilike(f'{value}%')
    #         elif operator == 'ends_with':
    #             return column.ilike(f'%{value}')
    #         elif operator == 'gt':
    #             return column > value
    #         elif operator == 'gte':
    #             return column >= value
    #         elif operator == 'lt':
    #             return column < value
    #         elif operator == 'lte':
    #             return column <= value
            
    #         return None
        
    #     except Exception as e:
    #         self.logger.error(f"Error building clause: {e}")
    #         return None
    
    
    def _build_single_clause(self, model_class, condition: Dict):
        """
        Build a single SQLAlchemy filter clause.
        Handles JSON-as-string columns for region/country using LIKE.
        """
        try:
            field = condition.get('field')
            operator = condition.get('operator', 'contains')
            value = condition.get('value')
            values = condition.get('values', [])
            
            if not field or not hasattr(model_class, field):
                self.logger.warning(f"Field '{field}' not found in model")
                return None
            
            column = getattr(model_class, field)
            
            # If region/country stored as JSON-string, use LIKE matching for 'region' and 'country' keys
            is_json_str_col = (
                '__hash__' in field
                or field.lower().endswith('region')
                or field.lower().endswith('country')
            )

            if is_json_str_col and operator in ['equals', 'in']:
                if operator == 'equals' and value:
                    # Match both single- and double-quotes for cross-language safety:
                    return or_(
                        column.ilike(f"%'{value}': True%"),
                        column.ilike(f'%"{value}": True%')
                    )
                elif operator == 'in' and values:
                    conds = [
                        or_(
                            column.ilike(f"%'{val}': True%"),
                            column.ilike(f'%"{val}": True%')
                        ) for val in values
                    ]
                    return or_(*conds)

            # Standard operators (for normal columns)
            if operator == 'contains':
                return column.ilike(f'%{value}%')
            elif operator in ['equals', '=']:
                return column == value
            elif operator == 'in':
                if values:
                    return column.in_(values)
            elif operator == 'between':
                if isinstance(value, list) and len(value) == 2:
                    return between(column, value[0], value[1])
            elif operator == 'starts_with':
                return column.ilike(f'{value}%')
            elif operator == 'ends_with':
                return column.ilike(f'%{value}')
            elif operator == 'gt':
                return column > value
            elif operator == 'gte':
                return column >= value
            elif operator == 'lt':
                return column < value
            elif operator == 'lte':
                return column <= value
            
            return None
        except Exception as e:
            self.logger.error(f"Error building clause: {e}")
            return None

    
    def _apply_sorting(self, query, model_class, sort_by: str, direction: str = 'asc'):
        """Apply sorting to query."""
        try:
            if not hasattr(model_class, sort_by):
                sort_by = 'primary_id'
            
            column = getattr(model_class, sort_by)
            
            if direction.lower() == 'desc':
                query = query.order_by(column.desc())
            else:
                query = query.order_by(column.asc())
            
            return query
        except Exception as e:
            return query
    
    def _serialize_record(self, record) -> Dict:
        """Serialize model to dict."""
        try:
            result = {}
            for column in record.__table__.columns:
                value = getattr(record, column.name)
                if isinstance(value, datetime):
                    result[column.name] = value.isoformat()
                else:
                    result[column.name] = value
            return result
        except Exception:
            return {}
    
    def _get_filter_counts(self, model_class, search: Dict) -> Dict:
        """Get filter counts for UI updates."""
        try:
            counts = {}
            base_query = db.session.query(model_class)
            if search and 'conditions' in search:
                base_query = self._apply_filters_recursive(
                    base_query, 
                    model_class, 
                    search['conditions'], 
                    search.get('logic', 'AND')
                )
            
            for filter_col in ['country', 'language', 'year', 'amstar_label']:
                if hasattr(model_class, filter_col):
                    column = getattr(model_class, filter_col)
                    count_query = db.session.query(
                        column,
                        func.count(column).label('count')
                    ).filter(column.isnot(None)).group_by(column)
                    
                    if search and 'conditions' in search:
                        count_query = self._apply_filters_recursive(
                            count_query, 
                            model_class, 
                            search['conditions'], 
                            search.get('logic', 'AND')
                        )
                    
                    results = count_query.all()
                    counts[filter_col.title()] = [
                        {'value': str(r[0]), 'count': r[1]} 
                        for r in results if r[0]
                    ]
            
            return counts
        except Exception:
            return {}
    
    def _handle_export(self, query, format: str, model_class):
        """Handle export."""
        records = query.all()
        
        if format == 'csv':
            return self._export_csv(records, model_class)
        elif format == 'json':
            return self._export_json(records)
        elif format == 'excel':
            return self._export_excel(records, model_class)
        else:
            return {'success': False, 'error': 'Invalid format'}, 400
    
    def _export_csv(self, records, model_class):
        """Export as CSV."""
        try:
            output = io.StringIO()
            if not records:
                return {'success': False, 'error': 'No records'}, 404
            
            columns = [col.name for col in model_class.__table__.columns]
            writer = csv.DictWriter(output, fieldnames=columns)
            writer.writeheader()
            
            for record in records:
                writer.writerow(self._serialize_record(record))
            
            output.seek(0)
            return send_file(
                io.BytesIO(output.getvalue().encode('utf-8')),
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            )
        except Exception as e:
            return {'success': False, 'error': str(e)}, 500
    
    def _export_json(self, records):
        """Export as JSON."""
        try:
            serialized = [self._serialize_record(r) for r in records]
            output = io.BytesIO(json.dumps(serialized, indent=2).encode('utf-8'))
            return send_file(
                output,
                mimetype='application/json',
                as_attachment=True,
                download_name=f'export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            )
        except Exception as e:
            return {'success': False, 'error': str(e)}, 500
    
    def _export_excel(self, records, model_class):
        """Export as Excel."""
        try:
            import openpyxl
            from openpyxl import Workbook
            
            wb = Workbook()
            ws = wb.active
            ws.title = "Export"
            
            if not records:
                return {'success': False, 'error': 'No records'}, 404
            
            columns = [col.name for col in model_class.__table__.columns]
            ws.append(columns)
            
            for record in records:
                row = self._serialize_record(record)
                ws.append([row.get(col) for col in columns])
            
            output = io.BytesIO()
            wb.save(output)
            output.seek(0)
            
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=f'export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            )
        except ImportError:
            return {'success': False, 'error': 'openpyxl not installed'}, 500
        except Exception as e:
            return {'success': False, 'error': str(e)}, 500