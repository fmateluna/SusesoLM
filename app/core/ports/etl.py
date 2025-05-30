from abc import ABC, abstractmethod
### repositorio para manejar el estado de las tareas. ###
class TaskRepository(ABC):
    def __init__(self, log_file: str = "etl.log"):
        self.__log_file = log_file  
        
    @abstractmethod
    def get_task_status(self, task_id: str) -> dict:
        pass
    
    @abstractmethod
    def set_task_status(self, task_id: str, status: str, detail: dict) -> None:
        pass
    
    def _get_log_file(self):
        return self.__log_file    