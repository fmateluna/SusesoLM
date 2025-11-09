from pydantic import BaseModel
from typing import Optional, Dict, Any

class ETL_Response(BaseModel):
    Status: str
    detail: Dict[str, Any]