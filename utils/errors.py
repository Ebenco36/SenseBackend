class BadRequestException(Exception):
    def __init__(self, message):
        super().__init__(message)

class DatabaseError(Exception):
    """Custom exception for database-related errors."""
    pass

class RecordNotFoundError(DatabaseError):
    """Raised when a specific record is not found."""
    pass