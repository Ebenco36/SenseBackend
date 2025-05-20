from flask_restful import Resource
from celery_app import celery
from src.Utils.response import ApiResponse

class TaskListAPI(Resource):
    def get(self):
        """
        List all Celery task instances across workers: 
        - active (currently running) 
        - scheduled (ETA/countdown) 
        - reserved (received but not yet started)
        """
        inspector = celery.control.inspect()

        # Each of these returns a dict: { worker_name: [<task dict>, …], … }
        active    = inspector.active()    or {}
        scheduled = inspector.scheduled() or {}
        reserved  = inspector.reserved()  or {}

        tasks = []

        # Active tasks
        for worker, items in active.items():
            for t in items:
                tasks.append({
                    "id":        t.get("id"),
                    "name":      t.get("name"),
                    "args":      t.get("args"),
                    "kwargs":    t.get("kwargs"),
                    "received":  t.get("time_start"),    # or t.get("received")
                    "worker":    worker,
                    "state":     "ACTIVE",
                })

        # Scheduled tasks (with ETA/countdown)
        for worker, items in scheduled.items():
            for entry in items:
                req = entry.get("request", {})
                tasks.append({
                    "id":       req.get("id"),
                    "name":     req.get("name"),
                    "args":     req.get("args"),
                    "kwargs":   req.get("kwargs"),
                    "eta":      entry.get("eta"),
                    "worker":   worker,
                    "state":    "SCHEDULED",
                })

        # Reserved tasks (waiting to be executed)
        for worker, items in reserved.items():
            for t in items:
                tasks.append({
                    "id":      t.get("id"),
                    "name":    t.get("name"),
                    "args":    t.get("args"),
                    "kwargs":  t.get("kwargs"),
                    "worker":  worker,
                    "state":   "RESERVED",
                })

        return ApiResponse.success(
            message="Current Celery task instances",
            data={"tasks": tasks},
            status_code=200
        )
    
    
class TaskStatusAPI(Resource):
    def get(self, task_id):
        """
        Check the status of a background task.
        """
        task = celery.AsyncResult(task_id)

        if task.state == 'PENDING':
            return ApiResponse.success(
                message="Task is still pending.",
                data={"status": task.state},
                status_code=200
            )
        elif task.state == 'SUCCESS':
            return ApiResponse.success(
                message="Task completed successfully.",
                data=task.result,
                status_code=200
            )
        elif task.state == 'FAILURE':
            return ApiResponse.error(
                message="Task failed.",
                data={"status": task.state, "error": str(task.info)},
                status_code=500
            )
        else:
            return ApiResponse.success(
                message="Task is in progress.",
                data={"status": task.state},
                status_code=200
            )