# src/Utils/response.py
"""
Standardized API Response Handler
"""

from flask import jsonify
from typing import Dict, Any, Optional, List
from flask import jsonify, make_response


class ApiResponse:
    """Unified API response formatter"""
    
    @staticmethod
    def success(data: Any = None, message: str = "Success", status_code: int = 200) -> tuple:
        """
        Return successful response
        
        Args:
            data: Response data
            message: Success message
            status_code: HTTP status code
            
        Returns:
            Tuple of (response_dict, status_code)
        """
        response = {
            'success': True,
            'message': message,
            'data': data
        }
        return make_response(jsonify(response), status_code)
    
    @staticmethod
    def error(message: str = "Error occurred", errors: Any = None, status_code: int = 400) -> tuple:
        """
        Return error response
        
        Args:
            message: Error message
            errors: Error details
            status_code: HTTP status code
            
        Returns:
            Tuple of (response_dict, status_code)
        """
        response = {
            'success': False,
            'message': message,
            'errors': errors
        }
        return make_response(jsonify(response), status_code)
    
    @staticmethod
    def paginated(items: List[Any], total: int, page: int, per_page: int, 
                  message: str = "Success", status_code: int = 200) -> tuple:
        """
        Return paginated response
        
        Args:
            items: List of items
            total: Total count
            page: Current page
            per_page: Items per page
            message: Success message
            status_code: HTTP status code
            
        Returns:
            Tuple of (response_dict, status_code)
        """
        total_pages = (total + per_page - 1) // per_page
        
        response = {
            'success': True,
            'message': message,
            'data': {
                'items': items,
                'pagination': {
                    'total': total,
                    'page': page,
                    'per_page': per_page,
                    'total_pages': total_pages,
                    'has_next': page < total_pages,
                    'has_prev': page > 1
                }
            }
        }
        return make_response(jsonify(response), status_code)
    
    
# class ApiResponse:
#     @staticmethod
#     def success(data={}, message='Success', status_code=200):
#         response = {
#             'status': 'success',
#             'message': message,
#             'data': data
#         }
#         return make_response(jsonify(response), status_code)

#     @staticmethod
#     def error(message='Error', status_code=400, errors={}):
#         response = {
#             'status': 'error',
#             'message': message,
#             'errors': errors
#         }
#         return make_response(jsonify(response), status_code)
