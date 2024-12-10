from flask import jsonify, make_response

class ApiResponse:
    @staticmethod
    def success(data={}, message='Success', status_code=200):
        response = {
            'status': 'success',
            'message': message,
            'data': data
        }
        return make_response(jsonify(response), status_code)

    @staticmethod
    def error(message='Error', status_code=400, errors={}):
        response = {
            'status': 'error',
            'message': message,
            'errors': errors
        }
        return make_response(jsonify(response), status_code)
