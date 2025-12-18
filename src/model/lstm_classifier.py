import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns 
import tempfile
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Input
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.metrics import classification_report, confusion_matrix
import mlflow
import mlflow.tensorflow
from mlflow.models.signature import infer_signature
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

class LSTMClassifier:
    def __init__(self, 
                 timesteps: int, n_features: int, n_classes: int = 3,
                 experiment_name: str = "LSTM_Classifier_V1",
                 tracking_uri: str = "http://mlflow:5000"):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.timesteps = timesteps
        self.n_features = n_features
        self.n_classes = n_classes
        self.model = self._build()

        self.lr: float = 1e-3

        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)
        self.run_id: str | None = None


    # MLflow helper functions
    def _start_or_resume_run(self):
        if mlflow.active_run() is not None:
            self.run_id = mlflow.active_run().info.run_id
            return
        
        if self.run_id is not None:
            mlflow.start_run(run_id=self.run_id)
            return
        
        run = mlflow.start_run()
        self.run_id = run.info.run_id


    def _end_run(self):
        if mlflow.active_run() is not None:
            mlflow.end_run()


    # Model
    def _build(self):
        self.logger.info("Building LSTM model... | input=(%d, %d) classes=%d", self.timesteps, self.n_features, self.n_classes)
        model = Sequential([
            Input(shape=(self.timesteps, self.n_features)),
            LSTM(64, return_sequences=True),
            LSTM(8),
            Dense(self.n_classes, activation="softmax")
        ])
        return model


    def compile(self, learning_rate: float = 5e-4):
        self.lr = learning_rate
        self.logger.info("Compiling model... | lr=%f", self.lr)
        self.model.compile(
            optimizer=Adam(learning_rate=self.lr),
            loss="categorical_crossentropy",
            metrics=["accuracy"]
        )

    
    # Training
    def fit(self, 
            X_train, y_train, X_val, y_val, 
            epochs: int, 
            batch_size: int,
            class_weights: dict | None = None,
            monitor: str = "val_loss", 
            min_delta: float = 1e-3, 
            patience: int = 5,
            metadata_dict: dict | None = None
    ):
        self.logger.info("Starting training...")
        self._start_or_resume_run()

        early_stop = EarlyStopping(
            monitor=monitor,
            min_delta=min_delta,
            patience=patience,
            restore_best_weights=True
        )

        history = self.model.fit(
            X_train, y_train,
            validation_data=(X_val, y_val),
            epochs=epochs,
            batch_size=batch_size,
            class_weight=class_weights,
            callbacks=[early_stop],
            verbose=1
        )

        # ---- log model (architecture, weights,...) ----
        mlflow.tensorflow.log_model(
            self.model,
            name="model",
            signature=infer_signature(X_val[:32], y_val[:32]),
            input_example=X_val[:32]    
        )

        # ---- log training info ----
        mlflow.log_params({
            "learning_rate": self.lr,
            "epochs": epochs,
            "batch_size": batch_size,
            "monitor": monitor,
            "min_delta": min_delta,
            "patience": patience
        })

        if class_weights is not None:
            mlflow.log_dict(class_weights, artifact_file="training/class_weight.json")

        if metadata_dict:
            mlflow.log_dict(metadata_dict, artifact_file="metadata.json")

        # ---- log train/val metrics per epoch ----
        for epoch in range(len(history.history["loss"])):
            mlflow.log_metrics(
                {
                    "train/loss": history.history["loss"][epoch],
                    "train/accuracy": history.history["accuracy"][epoch],
                    "val/loss": history.history["val_loss"][epoch],
                    "val/accuracy": history.history["val_accuracy"][epoch],
                },
                step=epoch
            )

        self._end_run()
        self.logger.info("Training finished")
        return history


    # Prediction
    def predict(self, X):
        return np.argmax(self.model.predict(X), axis=1)


    # Evaluation
    def log_classification_report(self, y_true, y_pred, labels=None, prefix: str = "test"):
        self.logger.info("Logging '%s' classification report to MLflow...", prefix)
        self._start_or_resume_run()

        report = classification_report(y_true, y_pred, labels=labels, output_dict=True)

        metrics = {
            f"{prefix}/accuracy": report["accuracy"]
        }

        # ---- macro & weighted ----
        for avg in ["macro avg", "weighted avg"]:
            for m in ["precision", "recall", "f1-score"]:
                metrics[f"{prefix}/{avg.replace(' ', '_')}/{m}"] = report[avg][m]

        # ---- per-class ----
        for cls, values in report.items():
            if cls in ["accuracy", "macro avg", "weighted avg"]:
                continue
            if not cls.isdigit():
                continue
            for m in ["precision", "recall", "f1-score"]:
                metrics[f"{prefix}/class_{cls}/{m}"] = values[m]

        mlflow.log_metrics(metrics)
        mlflow.log_dict(
            report,
            f"evaluation/{prefix}_classification_report.json"
        )

        self._end_run()
        self.logger.info("Logged '%s' classification report", prefix)
        return report


    def log_confusion_matrix(self, y_true, y_pred, labels=None, prefix: str = "test"):
        self.logger.info("Logging '%s' confusion matrix to MLflow...", prefix)
        self._start_or_resume_run()

        # Compute confusion matrix
        cm = confusion_matrix(y_true, y_pred, labels=labels)
        if labels is None:
            labels = list(map(str, range(cm.shape[0])))

        # Plot heatmap
        fig, ax = plt.subplots(figsize=(4, 4))
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", xticklabels=labels, yticklabels=labels, ax=ax)
        ax.set_xlabel("Predicted")
        ax.set_ylabel("True")
        ax.set_title(f"Confusion Matrix of {prefix.capitalize()} Set")

        mlflow.log_figure(fig, artifact_file=f"evaluation/{prefix}_confusion_matrix.png")
        plt.close(fig)

        self._end_run()
        self.logger.info("Logged '%s' confusion matrix", prefix)
        return cm
