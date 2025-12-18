from trino_utils import TrinoUtils
from lstm_preprocessor import LSTMPreprocessor
from lstm_classifier import LSTMClassifier
from lstm_promoter import LSTMPromoter
import numpy as np

def main():
    # 1. Read data from Trino
    trino_utils = TrinoUtils(catalog="iceberg", schema="gold")
    df = trino_utils.read_table(table="fact_daily_ohlcv")

    # 2. Preprocess for LSTM
    features = [
        'return', 'body_ratio', 'upper_ratio', 'lower_ratio', 'is_green', 
        'ema_10_dist_pct', 'ema_20_dist_pct', 'rsi_14_scaled', "rvol_10"
    ]
    label = "label_2"
    preprocessor = LSTMPreprocessor(features=features, label=label, timesteps=10, num_classes=3, val_days=120)
    data_dict, metadata_dict = preprocessor.fit_transform(df, return_dates=True)

    # 3. Build and train LSTM
    X_train = data_dict["X_train"]
    y_train = data_dict["y_train"]
    X_val = data_dict["X_val"]
    y_val = data_dict["y_val"]
    class_weights = data_dict["class_weights"]

    lstm_model = LSTMClassifier(
        timesteps=X_train.shape[1], 
        n_features=X_train.shape[2], 
        n_classes=y_train.shape[1],
        experiment_name="LSTM_Classifier_V2",
    )
    lstm_model.compile(learning_rate=5e-4)
    history = lstm_model.fit(
        X_train, y_train, X_val, y_val,
        epochs=50,
        batch_size=64,
        class_weights=class_weights,
        monitor="val_loss",
        min_delta=0.001,
        patience=10,
        metadata_dict=metadata_dict
    )

    # 4. Evaluate & log metrics
    y_pred = lstm_model.predict(X_val)
    lstm_model.log_classification_report(np.argmax(y_val, axis=1), y_pred)
    lstm_model.log_confusion_matrix(np.argmax(y_val, axis=1), y_pred)

    # 5. Promote model via MLflow
    promoter = LSTMPromoter(
        registry_model_name="LSTM_Classifier_Model",
        aliases=("production", "staging", "challenger"),
        metric_from_report=("macro avg", "f1-score"),
        threshold=0.33
    )

    run_id = lstm_model.run_id
    promoter.promote(run_id, X_val, y_val)


if __name__ == "__main__":
    main()
