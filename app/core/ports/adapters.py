

from app.core.ports.etl import TaskRepository


class InMemoryTaskRepository(TaskRepository ):
    
    def __init__(self, log_file: str):
        super().__init__(log_file)
        self.task_status = {}
    
    def get_task_status(self, task_id: str) -> dict:
        return self.task_status.get(task_id, None)
    
    def set_task_status(self, task_id: str, status: str, detail: dict) -> None:
        self.task_status[task_id] = {"Status": status, "detail": detail}