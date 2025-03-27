from abc import ABC, abstractmethod
### repositorio para manejar el estado de las tareas. ###
class TaskRepository(ABC):
    @abstractmethod
    def get_task_status(self, task_id: str) -> dict:
        pass
    
    @abstractmethod
    def set_task_status(self, task_id: str, status: str, detail: dict) -> None:
        pass