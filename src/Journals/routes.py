from src.Journals.views.data_controller import (
    DataProcessorResource, 
    DataProcessorFilterResource, 
    DataFilterResource
)

from src.Journals.views.index import RecordsAPI
from src.Journals.views.data_filters import (
    FilterAPI, ProcessUserSelectionAPI,
    SummaryStatisticsAPI, FetchRecordAPI
)
from src.Journals.views.paper_processor_api import PaperProcessorAPI, PaperProcessorExecute
from src.Journals.views.task_status_api import TaskStatusAPI, TaskListAPI
def journal_routes(api):
    api.add_resource(RecordsAPI, '/api/records', '/api/records/<int:record_id>')
    api.add_resource(FilterAPI, '/api/record/filters', '/api/record/filters')
    api.add_resource(
        ProcessUserSelectionAPI, 
        '/api/user-selection-process', 
        '/api/user-selection-process'
    )
    api.add_resource(SummaryStatisticsAPI, '/api/summary_statistics')
    api.add_resource(FetchRecordAPI, '/api/details/<int:id>')
    api.add_resource(DataProcessorFilterResource, '/data-filter/<int:row_id>')
    api.add_resource(DataProcessorResource, '/data/processing')
    # api.add_resource(DataFilterResource, '/data/filter')
    # Add the resource to the API
    api.add_resource(PaperProcessorAPI, '/api/process-uploaded-file')
    api.add_resource(PaperProcessorExecute, '/api/execute')

    api.add_resource(TaskStatusAPI, '/api/task-status-api')
    api.add_resource(TaskListAPI, "/api/tasks")