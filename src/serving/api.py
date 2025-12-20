import os
import time
import logging
from threading import Lock
from typing import List
import numpy as np
import mlflow.pyfunc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import shutil

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

MODEL_URI = os.getenv("MODEL_URI", "models:/LSTM_Classifier_Model@production")
LOCAL_MODEL_PATH = "/models/lstm_classifier_model"

_model_cache = None
_model_lock = Lock()


def load_model(retries=5, delay=30, force_reload=False):
    global _model_cache

    # Check cache first
    if _model_cache is not None and not force_reload:
        logger.info("[MODEL] Using cached model in memory")
        return _model_cache

    # Check local disk, if exists, then load from there
    if os.path.exists(LOCAL_MODEL_PATH) and not force_reload:
        logger.info(f"[MODEL] Loading model from local cache: {LOCAL_MODEL_PATH}")
        model = mlflow.pyfunc.load_model(LOCAL_MODEL_PATH)
        with _model_lock:
            _model_cache = model
        return _model_cache

    # Downloading model from MLflow if not cached or not on disk
    last_exception = None
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"[MLFLOW] Downloading new model from {MODEL_URI} (Attempt {attempt}/{retries})")
            new_model = mlflow.pyfunc.load_model(MODEL_URI)
            logger.info("[MLFLOW] New model downloaded from MLflow")

            tmp_path = f"{LOCAL_MODEL_PATH}_tmp"
            # Clean up temp path if exists
            if os.path.exists(tmp_path):
                shutil.rmtree(tmp_path)
            # Save model to temp path
            new_model.save_model(tmp_path)
            logger.info(f"[MLFLOW] New model saved to temporary path: {tmp_path}")

            # Delete old model
            if os.path.exists(LOCAL_MODEL_PATH):
                shutil.rmtree(LOCAL_MODEL_PATH)
            # Rename temp to official path
            os.rename(tmp_path, LOCAL_MODEL_PATH)
            logger.info(f"[MLFLOW] Temporary path {tmp_path} renamed to {LOCAL_MODEL_PATH}")

            with _model_lock:
                _model_cache = new_model

            return _model_cache

        except Exception as e:
            last_exception = e
            logger.warning(f"[MLFLOW] Load attempt {attempt} failed: {e}")
            time.sleep(delay)

    raise RuntimeError(f"Failed to load MLflow model after {retries} retries") from last_exception


# --- FastAPI ---
app = FastAPI(title="LSTM Classifier API")

class PredictRequest(BaseModel):
    inputs: List[List[List[float]]]  # shape: (batch_size, timesteps, features)

class PredictResponse(BaseModel):
    predictions: List[List[float]]  # shape: (batch_size, num_classes)


@app.on_event("startup")
def startup_event():
    load_model()


@app.get("/health")
def health():
    if _model_cache is not None:
        return {"status": "ok"}
    return {"status": "model not loaded"}


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest):
    try:
        arr_inputs = np.array(request.inputs, dtype=np.float64)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot convert inputs to numpy array: {e}")

    if arr_inputs.ndim != 3:
        raise HTTPException(status_code=400, detail="Input must be 3D: (batch_size, timesteps, features)")

    model = _model_cache
    if model is None:
        raise HTTPException(status_code=500, detail="Model not loaded")
    
    try:
        probs = model.predict(arr_inputs)  # shape: (batch_size, num_classes)
    except Exception as e:
        logger.error(f"Model prediction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Model prediction error: {e}")

    return PredictResponse(predictions=probs.tolist())


@app.post("/reload-model")
def reload_model():
    load_model(force_reload=True)
    return {"status": "model reloaded"}
