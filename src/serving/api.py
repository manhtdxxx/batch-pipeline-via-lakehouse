import os
import time
import logging
from threading import Lock
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import numpy as np
import mlflow.pyfunc
import shutil
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

MLFLOW_MODEL_URI = os.getenv("MLFLOW_MODEL_URI", "models:/LSTM_Classifier_Model@production")
LOCAL_MODEL_ROOT = "/app/lstm_classifier_models"
LOCAL_MODEL_PATH = os.path.join(LOCAL_MODEL_ROOT, "model")

_model_cache = None
_model_lock = Lock()


def load_model(retries=5, delay=30, force_reload=False):
    global _model_cache

    with _model_lock:
            
        # Check cache first
        if _model_cache is not None and not force_reload:
            logger.info("[MODEL] Using cached model in memory")
            return _model_cache

        # Check local disk, if exists, then load from there
        if os.path.exists(LOCAL_MODEL_PATH) and not force_reload:
            logger.info(f"[MODEL] Loading existing model from local disk at {LOCAL_MODEL_PATH}, not re-downloading")
            _model_cache = mlflow.pyfunc.load_model(LOCAL_MODEL_PATH)
            return _model_cache

        # Downloading model from MLflow if not cached or not on disk
        last_exception = None
        for attempt in range(1, retries + 1):
            try:
                tmp = LOCAL_MODEL_ROOT + "_tmp"
                if os.path.exists(tmp):
                    shutil.rmtree(tmp)

                logger.info(f"[MLFLOW] Downloading new model from {MLFLOW_MODEL_URI} (Attempt {attempt}/{retries})")
                mlflow.artifacts.download_artifacts(artifact_uri=MLFLOW_MODEL_URI, dst_path=tmp)
                logger.info(f"[MLFLOW] New model downloaded from MLflow, saved at {tmp}")

                if os.path.exists(LOCAL_MODEL_ROOT):
                    shutil.rmtree(LOCAL_MODEL_ROOT)
                    logger.info(f"[MODEL] Removed old model at {LOCAL_MODEL_ROOT}")

                os.rename(tmp, LOCAL_MODEL_ROOT)
                logger.info(f"[MODEL] Renamed {tmp} to {LOCAL_MODEL_ROOT}")

                _model_cache = mlflow.pyfunc.load_model(LOCAL_MODEL_PATH)
                logger.info("[MODEL] New model loaded into memory")
                
                return _model_cache

            except Exception as e:
                last_exception = e
                logger.exception(f"[MLFLOW] Failed to load model from MLflow: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
        
        raise RuntimeError(f"[MLFLOW] Failed to load model from MLflow after {retries} retries") from last_exception


# --- FastAPI ---
app = FastAPI(title="LSTM Classifier API")

class PredictRequest(BaseModel):
    inputs: List[List[List[float]]]  # shape: (batch_size, timesteps, features)

class PredictResponse(BaseModel):
    predictions: List[List[float]]  # shape: (batch_size, num_classes)


@app.on_event("startup")
def startup_event():
    try:
        load_model()
    except Exception as e:
        logger.exception(f"Failed to load model on startup: {e}")
        raise RuntimeError(f"Failed to load model on startup: {e}") from e


@app.get("/health")
def health():
    if _model_cache is not None:
        return {"status": "model loaded"}
    return {"status": "model not loaded"}


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest):
    if _model_cache is None:
        raise HTTPException(status_code=500, detail="Model not loaded")
    
    try:
        arr_inputs = np.array(request.inputs, dtype=np.float64)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot convert inputs to numpy array: {e}")

    if arr_inputs.ndim != 3:
        raise HTTPException(status_code=400, detail="Input must be 3D: (batch_size, timesteps, features)")
    
    try:
        model = _model_cache  # in case of using old model when _model_cache is reloading
        probs = model.predict(arr_inputs)  # shape: (batch_size, num_classes)
    except Exception as e:
        logger.exception(f"Failed to make prediction: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to make prediction: {e}")

    return PredictResponse(predictions=probs.tolist())


@app.post("/reload-model")
def reload_model():
    try:
        load_model(force_reload=True)
    except Exception as e:
        logger.exception(f"Failed to reload model: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to reload model: {e}")
    return {"status": "model reloaded"}