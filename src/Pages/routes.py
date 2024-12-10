from src.Pages.views.contact import (
    ContactFormResource
)

def pages_routes(api):
    api.add_resource(ContactFormResource, '/contact')