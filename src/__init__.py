from flask import Flask, Blueprint
from flask_restful import Api
from src.Journals.routes import journal_routes

class RouteInitialization:
    def __init__(self):
        self.blueprints = [
            {
                "name": "journals", 
                "blueprint":  Blueprint('Journals', __name__, static_url_path="assets"), 
                "register_callback": journal_routes, 
                "url_prefix": "/api/v1"
            },
            
        ]


    def init_app(self, flask_app: Flask):
        for blueprint in self.blueprints:
            init_route = Api(blueprint.get("blueprint"))
            blueprint.get("register_callback")(init_route)
            flask_app.register_blueprint(blueprint.get("blueprint"), url_prefix=blueprint.get("url_prefix"))

