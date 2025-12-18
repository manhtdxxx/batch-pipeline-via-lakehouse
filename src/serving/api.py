import mlflow.pyfunc
from fastapi import FastAPI
from pydantic import BaseModel
import numpy as np

app = FastAPI()

model = mlflow.pyfunc.load_model("models:/LSTM_Classifier_Model/production")

class InputData(BaseModel):
    data: list

@app.post("/predict")
def predict(input_data: InputData):
    X = np.array(input_data.data)
    preds = model.predict(X)
    return {"prediction": preds.tolist()}
