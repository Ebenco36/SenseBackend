# src/Services/BaseService.py

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseService(ABC):
    """
    Abstract base service class with common functionality
    
    All services inherit from this to ensure:
    - Consistent error handling
    - Logging
    - Response format
    - Validation
    """
    
    def __init__(self, name: str = None):
        """Initialize service with optional name"""
        self.service_name = name or self.__class__.__name__
        self.logger = logging.getLogger(self.service_name)
    
    @abstractmethod
    def validate_input(self, data: Any) -> tuple[bool, Optional[str]]:
        """
        Validate input data
        
        Returns: (is_valid, error_message)
        """
        pass
    
    def log_info(self, message: str, **kwargs):
        """Log info level message"""
        self.logger.info(f"[{self.service_name}] {message}", extra=kwargs)
    
    def log_warning(self, message: str, **kwargs):
        """Log warning level message"""
        self.logger.warning(f"[{self.service_name}] {message}", extra=kwargs)
    
    def log_error(self, message: str, exception: Exception = None, **kwargs):
        """Log error level message"""
        if exception:
            self.logger.error(
                f"[{self.service_name}] {message}: {str(exception)}",
                exc_info=True,
                extra=kwargs
            )
        else:
            self.logger.error(f"[{self.service_name}] {message}", extra=kwargs)
    
    def create_response(self, success: bool, data: Any = None, 
                       message: str = None, errors: List[str] = None) -> Dict:
        """
        Create standardized response format
        
        Returns:
        {
            'success': bool,
            'data': any,
            'message': str,
            'errors': list,
            'timestamp': datetime
        }
        """
        return {
            'success': success,
            'data': data,
            'message': message or ('Success' if success else 'Failed'),
            'errors': errors or [],
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def safe_execute(self, func, *args, **kwargs):
        """
        Safely execute function with error handling
        
        Usage:
            result = service.safe_execute(some_function, arg1, arg2)
        """
        try:
            return func(*args, **kwargs)
        except Exception as e:
            self.log_error(f"Error executing {func.__name__}", exception=e)
            raise