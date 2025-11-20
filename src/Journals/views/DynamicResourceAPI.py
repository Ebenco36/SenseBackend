# src/API/Resources.py (DYNAMIC VERSION)
"""
Flask-RESTful Resource Classes - DYNAMIC VERSION
Single BaseResourceAPI handles ANY table without subclassing
"""

from flask import request, current_app
from flask_restful import Resource, Api
from src.Journals.Services.ResourceService import ResourceService
from src.Utils.response import ApiResponse
import json
from typing import Optional, List, Dict, Any
from sqlalchemy import inspect, or_, and_

# http://127.0.0.1:5400/api/v1/api/all_db?search={"conditions":[{"field":"year","operator":"in","values":[2020,2021,2022,2023]},{"field":"title","operator":"contains","value":"vaccine"}],"logic":"AND"}
class DynamicResourceAPI(Resource):
    """Universal Dynamic Resource API with flexible search"""
    
    def __init__(self, searchable_fields=None, filter_fields=None):
        self.searchable_fields_config = searchable_fields or {}
        self.filter_fields_config = filter_fields or {}
    
    def _get_registry(self):
        """Get registry from app context"""
        return current_app.config['MODEL_REGISTRY']
    
    def _get_model_and_repo(self, table_name: str):
        """Get model class and repository for table"""
        registry = self._get_registry()
        
        try:
            model_class = registry.get_by_table_or_fail(table_name)
            repository = registry.get_repository(table_name)
            return model_class, repository
        except ValueError as e:
            raise ValueError(f"Table '{table_name}' not found: {str(e)}")
    
    def _get_all_columns(self, model_class) -> List[str]:
        """
        Get ALL column names from model
        This enables searching ANY column without configuration
        """
        mapper = inspect(model_class)
        return [col.name for col in mapper.columns]
    
    def _parse_search_query(self, search_data: Dict[str, Any], model_class) -> Optional[Dict]:
        """
        Parse flexible search query
        
        Supports multiple formats:
        
        Format 1: Simple search on auto-detected fields
        {
            "query": "search term"
        }
        
        Format 2: Search on specific fields
        {
            "query": "search term",
            "fields": ["title", "abstract", "authors"]
        }
        
        Format 3: Advanced search with operators
        {
            "conditions": [
                {
                    "field": "year",
                    "operator": "=",
                    "value": 2023
                },
                {
                    "field": "title",
                    "operator": "contains",
                    "value": "covid"
                }
            ],
            "logic": "AND"  # or "OR"
        }
        
        Format 4: Search multiple values (OR logic by default)
        {
            "field": "year",
            "values": [2020, 2021, 2022, 2023],
            "operator": "in"  # or "=" for OR logic
        }
        
        Format 5: Complex nested queries
        {
            "conditions": [
                {
                    "field": "year",
                    "operator": "in",
                    "values": [2020, 2021, 2022, 2023]
                },
                {
                    "field": "title",
                    "operator": "contains",
                    "value": "vaccine"
                },
                {
                    "field": "authors",
                    "operator": "contains",
                    "value": "Smith"
                }
            ],
            "logic": "AND"
        }
        """
        return search_data
    
    def _build_dynamic_search(self, repository, search_query: Dict[str, Any], model_class):
        """
        Build dynamic search query from flexible format
        
        Returns query builder with conditions applied
        """
        query = repository.new_query()
        
        # Format 1 & 2: Simple search
        if "query" in search_query:
            search_term = search_query["query"]
            fields = search_query.get("fields", [])
            
            # If no fields specified, search ALL string columns
            if not fields:
                mapper = inspect(model_class)
                from sqlalchemy import String, Text
                fields = [
                    col.name for col in mapper.columns
                    if isinstance(col.type, (String, Text))
                ]
            
            # Search with OR logic across fields
            if fields:
                for field in fields:
                    query.or_where(field, 'ilike', f'%{search_term}%')
            
            return query
        
        # Format 3, 4, 5: Advanced search
        if "conditions" in search_query:
            conditions = search_query["conditions"]
            logic = search_query.get("logic", "AND").upper()
            
            if not conditions:
                return query
            
            # Build all conditions
            built_conditions = []
            
            for condition in conditions:
                field = condition.get("field")
                operator = condition.get("operator", "=").lower()
                value = condition.get("value")
                values = condition.get("values", [])
                
                if not field:
                    continue
                
                # Single value conditions
                if operator == "=":
                    built_conditions.append(("where", field, "=", value))
                
                elif operator == "!=":
                    built_conditions.append(("where", field, "!=", value))
                
                elif operator == ">":
                    built_conditions.append(("where", field, ">", value))
                
                elif operator == ">=":
                    built_conditions.append(("where", field, ">=", value))
                
                elif operator == "<":
                    built_conditions.append(("where", field, "<", value))
                
                elif operator == "<=":
                    built_conditions.append(("where", field, "<=", value))
                
                elif operator == "contains" or operator == "like":
                    built_conditions.append(("where", field, "ilike", f"%{value}%"))
                
                elif operator == "starts_with":
                    built_conditions.append(("where", field, "ilike", f"{value}%"))
                
                elif operator == "ends_with":
                    built_conditions.append(("where", field, "ilike", f"%{value}"))
                
                # Multiple value conditions
                elif operator == "in":
                    if values:
                        built_conditions.append(("where_in", field, values))
                    elif value:
                        # Handle array in single value
                        if isinstance(value, list):
                            built_conditions.append(("where_in", field, value))
                        else:
                            built_conditions.append(("where", field, "=", value))
                
                elif operator == "not_in":
                    if values:
                        built_conditions.append(("where_not_in", field, values))
                
                elif operator == "is_null":
                    built_conditions.append(("where_null", field))
                
                elif operator == "is_not_null":
                    built_conditions.append(("where_not_null", field))
                
                elif operator == "between":
                    if isinstance(value, list) and len(value) == 2:
                        built_conditions.append(("where_between", field, value[0], value[1]))
            
            # Apply conditions with logic
            if logic == "OR":
                # For OR, we need to use or_where
                for i, condition in enumerate(built_conditions):
                    if condition[0] == "where":
                        if i == 0:
                            query.where(condition[1], condition[2], condition[3])
                        else:
                            query.or_where(condition[1], condition[2], condition[3])
                    
                    elif condition[0] == "where_in":
                        if i == 0:
                            query.where_in(condition[1], condition[2])
                        else:
                            # Note: or_where doesn't support in, so we use where with nested
                            pass
            else:
                # For AND, use regular where
                for condition in built_conditions:
                    if condition[0] == "where":
                        query.where(condition[1], condition[2], condition[3])
                    
                    elif condition[0] == "where_in":
                        query.where_in(condition[1], condition[2])
                    
                    elif condition[0] == "where_not_in":
                        query.where_not_in(condition[1], condition[2])
                    
                    elif condition[0] == "where_null":
                        query.where_null(condition[1])
                    
                    elif condition[0] == "where_not_null":
                        query.where_not_null(condition[1])
                    
                    elif condition[0] == "where_between":
                        query.where_between(condition[1], condition[2], condition[3])
            
            return query
        
        # Single field, multiple values
        if "field" in search_query and "values" in search_query:
            field = search_query["field"]
            values = search_query["values"]
            operator = search_query.get("operator", "in").lower()
            
            if operator == "in" or operator == "=":
                query.where_in(field, values)
            elif operator == "not_in":
                query.where_not_in(field, values)
            
            return query
        
        return query
    
    def get(self, table_name: str, record_id: Optional[int] = None):
        """
        GET endpoint with flexible search
        
        Supports multiple query parameter formats:
        
        1. Simple search (auto-detects string columns):
           ?q=search_term
        
        2. Search specific fields:
           ?q=search_term&fields=title,abstract,authors
        
        3. Advanced search (JSON):
           ?search={"conditions":[{"field":"year","operator":"=","value":2023}],"logic":"AND"}
        
        4. Multi-value search (all string columns):
           ?years=2020,2021,2022,2023
        
        5. Complex nested:
           ?search={"conditions":[
               {"field":"year","operator":"in","values":[2020,2021,2022]},
               {"field":"title","operator":"contains","value":"vaccine"}
           ],"logic":"AND"}
        """
        try:
            model_class, repository = self._get_model_and_repo(table_name)
            service = ResourceService(repository, model_class)
            
            # Get single record by ID
            if record_id is not None:
                response = service.get_by_id(record_id)
                if response['success']:
                    return ApiResponse.success(
                        data=response['data'],
                        message=response['message']
                    )
                else:
                    return ApiResponse.error(
                        message=response['message'],
                        errors=response.get('error'),
                        status_code=404
                    )
            
            # Pagination
            page = request.args.get('page', 1, type=int)
            per_page = request.args.get('per_page', 20, type=int)
            sort_by = request.args.get('sort_by', None)
            sort_direction = request.args.get('sort_direction', 'asc')
            
            # ⭐ FLEXIBLE SEARCH
            query = repository.new_query()
            
            # Priority 1: Advanced JSON search
            search_json = request.args.get('search', None)
            if search_json:
                try:
                    search_data = json.loads(search_json)
                    query = self._build_dynamic_search(repository, search_data, model_class)
                except Exception as e:
                    return ApiResponse.error(
                        message="Invalid search JSON",
                        errors=str(e),
                        status_code=400
                    )
            
            # Priority 2: Simple query parameter search
            else:
                simple_q = request.args.get('q', '')
                if simple_q:
                    # Search ALL columns
                    all_columns = self._get_all_columns(model_class)
                    for column in all_columns:
                        query.or_where(column, 'ilike', f'%{simple_q}%')
            
            # Apply sorting and pagination
            if sort_by:
                query.order_by(sort_by, sort_direction)
            
            results = query.paginate(page=page, per_page=per_page)
            
            # Serialize items
            from src.Services.DBservices.ModelSerializer import ModelSerializer
            serialized_items = [ModelSerializer.to_dict(item) for item in results.items]
            
            return ApiResponse.success(
                data={
                    'items': serialized_items,
                    'pagination': {
                        'total': results.total,
                        'page': results.page,
                        'per_page': results.per_page,
                        'total_pages': results.pages,
                        'has_next': results.has_next,
                        'has_prev': results.has_prev
                    }
                },
                message=f'Found {results.total} {table_name} records'
            )
        
        except ValueError as e:
            return ApiResponse.error(
                message=f"Table '{table_name}' not found",
                errors=str(e),
                status_code=404
            )
        except Exception as e:
            return ApiResponse.error(
                message=f"Search failed",
                errors=str(e)
            )
    
    # ========================================================================
    # POST - Create record
    # ========================================================================
    
    def post(self, table_name: str):
        """POST endpoint for creating new record"""
        try:
            model_class, repository = self._get_model_and_repo(table_name)
            service = ResourceService(repository, model_class)
            
            data = request.get_json()
            
            if not data:
                return ApiResponse.error(
                    message="Request body is required",
                    status_code=400
                )
            
            # Handle bulk create
            if isinstance(data, list):
                response = service.create_many(data)
            else:
                response = service.create(**data)
            
            if response['success']:
                return ApiResponse.success(
                    data=response['data'],
                    message=response['message'],
                    status_code=201
                )
            else:
                return ApiResponse.error(
                    message=response['message'],
                    errors=response.get('error'),
                    status_code=400
                )
        
        except ValueError as e:
            return ApiResponse.error(
                message=f"Table '{table_name}' not found",
                errors=str(e),
                status_code=404
            )
        except Exception as e:
            return ApiResponse.error(
                message=f"Failed to create record in {table_name}",
                errors=str(e),
                status_code=400
            )
    
    # ========================================================================
    # PUT/PATCH - Update record
    # ========================================================================
    
    def put(self, table_name: str, record_id: int):
        """PUT endpoint for updating record"""
        try:
            model_class, repository = self._get_model_and_repo(table_name)
            service = ResourceService(repository, model_class)
            
            data = request.get_json()
            
            if not data:
                return ApiResponse.error(
                    message="Request body is required",
                    status_code=400
                )
            
            response = service.update(record_id, **data)
            
            if response['success']:
                return ApiResponse.success(
                    data=response['data'],
                    message=response['message']
                )
            else:
                status_code = 404 if 'not found' in response['message'].lower() else 400
                return ApiResponse.error(
                    message=response['message'],
                    errors=response.get('error'),
                    status_code=status_code
                )
        
        except ValueError as e:
            return ApiResponse.error(
                message=f"Table '{table_name}' not found",
                errors=str(e),
                status_code=404
            )
        except Exception as e:
            return ApiResponse.error(
                message=f"Failed to update record in {table_name}",
                errors=str(e),
                status_code=400
            )
    
    patch = put  # PATCH uses same logic as PUT
    
    # ========================================================================
    # DELETE - Delete record
    # ========================================================================
    
    def delete(self, table_name: str, record_id: int):
        """DELETE endpoint for deleting record"""
        try:
            model_class, repository = self._get_model_and_repo(table_name)
            service = ResourceService(repository, model_class)
            
            response = service.delete(record_id)
            
            if response['success']:
                return ApiResponse.success(
                    message=response['message']
                )
            else:
                status_code = 404 if 'not found' in response['message'].lower() else 400
                return ApiResponse.error(
                    message=response['message'],
                    errors=response.get('error'),
                    status_code=status_code
                )
        
        except ValueError as e:
            return ApiResponse.error(
                message=f"Table '{table_name}' not found",
                errors=str(e),
                status_code=404
            )
        except Exception as e:
            return ApiResponse.error(
                message=f"Failed to delete record from {table_name}",
                errors=str(e),
                status_code=400
            )