import json
from datetime import datetime
from typing import Any

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def safe_json_dumps(data: Any, **kwargs) -> str:
    """Safely serialize data containing datetime objects"""
    return json.dumps(data, cls=DateTimeEncoder, **kwargs)

def ensure_serializable(data: Any) -> Any:
    """Recursively convert datetime objects to ISO strings"""
    if isinstance(data, datetime):
        return data.isoformat()
    elif isinstance(data, dict):
        return {k: ensure_serializable(v) for k, v in data.items()}
    elif isinstance(data, (list, tuple, set)):
        return [ensure_serializable(item) for item in data]
    return data