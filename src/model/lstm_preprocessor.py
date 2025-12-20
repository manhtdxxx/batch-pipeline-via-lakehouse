import numpy as np
import pandas as pd
from tensorflow.keras.utils import to_categorical
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

class LSTMPreprocessor:
    def __init__(self, features: list[str], label: str, timesteps: int = 10, num_classes: int = 3, val_days: int = 120):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.features = features
        self.label = label
        self.timesteps = timesteps
        self.num_classes = num_classes
        self.val_days = val_days


    def split_temporally(self, df: pd.DataFrame):
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna().sort_values(["symbol", "date"])

        last_date = df["date"].max()
        split_date = last_date - pd.Timedelta(days=self.val_days)

        df_train = df[df["date"] <= split_date].reset_index(drop=True)
        df_val   = df[df["date"] >  split_date].reset_index(drop=True)

        self.logger.info("Splitted temporally | train=%s val=%s", df_train.shape, df_val.shape)
        self.logger.info(
            "Train date range: [%s → %s] | Val date range: (%s → %s]",
            df_train["date"].min().date(),
            df_train["date"].max().date(),
            df_val["date"].min().date(),
            df_val["date"].max().date()
        )
        return df_train, df_val

  
    def create_sequences(self, df: pd.DataFrame):
        X_list, y_list = [], []

        for symbol in df["symbol"].unique():
            df_symbol = df[df["symbol"] == symbol].sort_values("date")
            X = df_symbol[self.features].values
            y = df_symbol[self.label].values
            for i in range(self.timesteps, len(X)):
                X_list.append(X[i-self.timesteps:i])
                y_list.append(y[i - 1])

        X_arr = np.array(X_list)
        y_arr = np.array(y_list)

        self.logger.debug(
            "Created sequences | samples=%d timesteps=%d features=%d",
            X_arr.shape[0], X_arr.shape[1], X_arr.shape[2]
        )
        return X_arr, y_arr
    

    def encode_labels(self, y: np.ndarray):
        y_cat = to_categorical(y, num_classes=self.num_classes)

        self.logger.debug(
            "Encoded labels | samples=%d num_classes=%d",
            y_cat.shape[0], y_cat.shape[1]
        )
        return y_cat


    def compute_class_weights(self, y_train: np.ndarray):
        classes, counts = np.unique(y_train, return_counts=True)
        total_samples = len(y_train)

        count_0 = counts[classes == 0][0]
        count_1 = counts[classes == 1][0]
        count_2 = counts[classes == 2][0]

        weight_1 = total_samples / (len(classes) * count_1)
        weight_0 = weight_2 = total_samples / (len(classes) * ((count_0 + count_2) / 2))

        class_weights_dict = {
            0: weight_0,
            1: weight_1,
            2: weight_2
        }

        self.logger.info(
            "Computed class weights | c0=%.4f c1=%.4f c2=%.4f",
            weight_0, weight_1, weight_2
        )
        return class_weights_dict

   
    def fit_transform(self, df: pd.DataFrame, return_dates: bool = False):
        self.logger.info("Starting preprocessing pipeline")

        df_train, df_val = self.split_temporally(df)

        X_train, y_train = self.create_sequences(df_train)
        X_val, y_val = self.create_sequences(df_val)

        y_train_cat = self.encode_labels(y_train)
        y_val_cat = self.encode_labels(y_val)

        class_weights = self.compute_class_weights(y_train)

        data_dict = {
            "X_train": X_train,
            "y_train": y_train_cat,
            "X_val": X_val,
            "y_val": y_val_cat,
            "class_weights": class_weights
        }

        metadata_dict = {
            "num_timesteps": self.timesteps,
            "num_features": len(self.features),
            "features": self.features,
            "label": self.label,
            "num_classes": self.num_classes,
            "num_samples": {
                "train": X_train.shape[0],
                "val": X_val.shape[0]
            },
            "class_distribution": {
                "train": dict(zip(*np.unique(y_train, return_counts=True))),
                "val": dict(zip(*np.unique(y_val, return_counts=True)))
            }
        }
        
        if return_dates:
            metadata_dict["dates"] = {
                "train_start": df_train["date"].min().date(),
                "train_end": df_train["date"].max().date(),
                "val_start": df_val["date"].min().date(),
                "val_end": df_val["date"].max().date()
            }

        self.logger.info(
            "Finished preprocessing | X_train=%s y_train=%s X_val=%s y_val=%s",
            X_train.shape, y_train_cat.shape, X_val.shape, y_val_cat.shape
        )

        return data_dict, metadata_dict