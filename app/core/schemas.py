from pydantic import BaseModel
from typing import Dict, List, Optional

class PredictionRequest(BaseModel):
    """Input schema for single prediction"""
    text: str

class BatchPredictionRequest(BaseModel):
    """Input schema for batch predictions"""
    texts: List[str]

class PredictionDetails(BaseModel):
    type: Optional[str] = None
    tags: List[str] = []
    url: Optional[str] = None

class PredictionResponse(BaseModel):
    """Output schema for all predictions"""
    label: str
    confidence: float
    details: PredictionDetails
    text: str | None = None 


class BatchPredictionResponse(BaseModel):
    """Wrapper for batch results"""
    results: List[PredictionResponse]