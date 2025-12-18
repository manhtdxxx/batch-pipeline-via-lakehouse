import logging
from typing import Tuple, List, Dict, Optional
import mlflow
import numpy as np
from mlflow.tracking import MlflowClient
from sklearn.metrics import classification_report


class LSTMPromoter:
    def __init__(
        self,
        registry_model_name: str,
        aliases: Tuple[str, ...] = ("production", "staging", "challenger"),
        metric_from_report: Tuple[str, str] = ("macro avg", "f1-score"),
        threshold: float = 0.33,
        tracking_uri: str = "http://mlflow:5000",
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = MlflowClient(tracking_uri=tracking_uri)
        self.registry_model_name = registry_model_name
        self.aliases = aliases
        self.metric_from_report = metric_from_report
        self.threshold = threshold


    # Load model to score
    def _load(self, uri: str):
        self.logger.debug("Loading model from %s...", uri)
        return mlflow.tensorflow.load_model(uri)


    # Score model
    def _score(self, model, X, y) -> float:
        assert y.ndim == 2, "y must be one-hot encoded"
        self.logger.debug("Scoring model...")

        y_pred = np.argmax(model.predict(X), axis=1)
        y_true = np.argmax(y, axis=1)
        report = classification_report(y_true, y_pred, output_dict=True, zero_division=0)

        section, metric = self.metric_from_report
        score = float(report.get(section, {}).get(metric, 0.0))
        return score


    # Create registered model if not exists
    def _ensure_registry_model_exists(self):
        try:
            self.client.get_registered_model(self.registry_model_name)
        except Exception:
            self.client.create_registered_model(self.registry_model_name)


    # Register new model version
    def _register_new_version(self, run_id: str) -> int:
        self.logger.debug("Registering new version from run_id=%s", run_id)
        self._ensure_registry_model_exists()
        mv = self.client.create_model_version(
            name=self.registry_model_name,
            source=f"runs:/{run_id}/model",
            run_id=run_id,
        )
        self.logger.debug("Registered new model version: %s", mv.version)
        return int(mv.version)


    # Set alias to version
    def _set_alias(self, alias: str, version: int):
        self.logger.debug("Setting alias '%s' to version %s", alias, version)
        self.client.set_registered_model_alias(
            name=self.registry_model_name,
            alias=alias,
            version=version,
        )
        

    # Set metric tag to version
    def _set_metric_tag(self, score: float, version: int):
        key = f"metric/{self.metric_from_report[0]}/{self.metric_from_report[1]}"
        self.logger.debug("Setting metric tag '%s' to version %s with score %.4f", key, version, score)
        self.client.set_model_version_tag(
            name=self.registry_model_name,
            version=version,
            key=key,
            value=f"{score:.4f}",
        )


    # Register challenger model if meets threshold
    def _register_challenger_model(self, run_id: str, X_eval, y_eval) -> Optional[int]:
        self.logger.info("Registering challenger model from run_id=%s", run_id)

        model = self._load(f"runs:/{run_id}/model")
        score = self._score(model, X_eval, y_eval)

        self.logger.info("New model score=%.4f", score)

        if score < self.threshold:
            self.logger.info("Score below threshold %.4f. Not registering model.", self.threshold)
            return None

        version = self._register_new_version(run_id)
        self._set_alias("challenger", version)
        self._set_metric_tag(score, version)
        return version
    

    # Evaluate existing alias model
    def _evaluate_alias(self, alias: str, X_eval, y_eval) -> Optional[Dict]:
        try:
            self.logger.info("Evaluating alias '%s'...", alias)
            mv = self.client.get_model_version_by_alias(self.registry_model_name, alias)
            model = self._load(f"models:/{self.registry_model_name}@{alias}")
            score = self._score(model, X_eval, y_eval)
            self._set_metric_tag(score, int(mv.version))
            return {
                "alias": alias, 
                "version": int(mv.version), 
                "score": score
            }
        except Exception as e:
            self.logger.warning("Could not evaluate alias '%s': %s", alias, e)
            return None


    # Rank evaluated models
    def _rank_models(self, scores: List[Dict]) -> List[Dict]:
        self.logger.debug("Ranking evaluated models...")
        ranked_scores = sorted(scores, key=lambda x: x["score"], reverse=True)
        return ranked_scores


    # Main promotion method
    def promote(self, run_id: str, X_eval, y_eval) -> bool:
        self.logger.info("Starting promotion for run_id=%s", run_id)

        new_version = self._register_challenger_model(run_id, X_eval, y_eval)
        if new_version is None:
            return False
        
        scores = []

        mv = self.client.get_model_version(self.registry_model_name, new_version)
        key = f"metric/{self.metric_from_report[0]}/{self.metric_from_report[1]}"
        score = float(mv.tags.get(key, 0.0))
        scores.append({
            "alias": "challenger",
            "version": new_version,
            "score": score
        })

        for alias in self.aliases:
            if alias == "challenger":
                continue
            s = self._evaluate_alias(alias, X_eval, y_eval)
            if s is not None:
                scores.append(s)

        if len(scores) == 0:
            self.logger.warning("No models to rank for promotion.")
            return False

        ranked_scores = self._rank_models(scores)

        # Set aliases according to ranking
        self._set_alias("challenger", new_version)
        self._set_alias("production", ranked_scores[0]["version"])
        if len(ranked_scores) > 1:
            self._set_alias("staging", ranked_scores[1]["version"])

        self.logger.info("Promotion completed. Production version: %s", ranked_scores[0]["version"])
        return True
