from src.Journals.views.data_controller import (
    DataProcessorResource, 
    DataProcessorFilterResource, 
    DataFilterResource
)

from src.Journals.views.index import RecordsAPI

def journal_routes(api):
    api.add_resource(RecordsAPI, '/api/records', '/api/records/<int:record_id>')
    api.add_resource(DataProcessorFilterResource, '/data-filter/<int:row_id>')
    api.add_resource(DataProcessorResource, '/data/processing')
    # api.add_resource(DataFilterResource, '/data/filter')