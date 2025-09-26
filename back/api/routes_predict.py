from fastapi import APIRouter
from core.config import settings


router = APIRouter(prefix="/predict", tags=["Predict"])

@router.post("/")
async def predict_price():
    return {settings.MODEL_NAME}