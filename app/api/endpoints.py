from fastapi import APIRouter
from app.core.schemas import PredictionRequest, BatchPredictionRequest, PredictionResponse, BatchPredictionResponse
from app.core.model import AnnouncementClassifier

router = APIRouter()
classifier = AnnouncementClassifier()

@router.post("/predict", response_model=PredictionResponse)
async def predict_single(request: PredictionRequest):
    return await classifier.predict(request)

@router.post("/predict/batch", response_model=BatchPredictionResponse)
async def predict_batch(request: BatchPredictionRequest):
    return await classifier.predict_batch(request)