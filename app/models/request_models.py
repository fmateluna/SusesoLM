from pydantic import BaseModel


class ETLRequest(BaseModel):
    start_date: str
    end_date: str