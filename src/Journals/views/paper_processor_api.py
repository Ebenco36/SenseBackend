# src/Resources/PaperProcessorAPI.py

import os
from flask import request
from flask_restful import Resource
from src.Journals.views.tasks import process_uploaded_file_task, process_uploaded_data_task
from src.Utils.response import ApiResponse

class PaperProcessorAPI(Resource):
    def post(self):
        # 1) ensure a file is present
        if 'file' not in request.files:
            return ApiResponse.error("No file provided", 400)
        f = request.files['file']
        if not f.filename.lower().endswith('.csv'):
            return ApiResponse.error("Only CSV files are supported", 400)

        # 2) save it temporarily
        temp_dir = 'temp'
        os.makedirs(temp_dir, exist_ok=True)
        temp_path = os.path.join(temp_dir, f.filename)
        f.save(temp_path)

        # 3) prepare the output path
        out_dir = os.path.join('Data', 'output')
        os.makedirs(out_dir, exist_ok=True)
        output_file = f'processed_{f.filename}'
        output_path = os.path.join(out_dir, output_file)

        # 4) enqueue the Celery task (only once!)
        task = process_uploaded_file_task.apply_async(
            args=[temp_path, output_path]
        )

        # 5) return immediately
        return ApiResponse.success(
            message="File accepted. Processing in background.",
            data={'task_id': task.id, 'output_file': output_file},
            status_code=202
        )


class PaperProcessorExecute(Resource):
    def post(self):
        # 1) ensure form data is present
        if not request.form:
            return ApiResponse.error("No form data provided", 400)
        data = request.form.to_dict()
        task = process_uploaded_data_task(data)

        # 4) return immediately with task info
        return ApiResponse.success(
            message="Data accepted.",
            data=task,
            status_code=200
        )