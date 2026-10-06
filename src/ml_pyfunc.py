"""MLflow pyfunc 모델 — 북쪽·동쪽 회귀 모델 두 개를 하나의 "이동량 예측 모델"로 묶는다. (0-8)

MLflow 레지스트리에는 모델 하나가 한 버전으로 올라간다. 우리 모델은 축마다 하나씩 두 개라,
두 개를 묶어 입력(특성 행렬) → 출력(north_m, east_m)인 하나의 pyfunc로 저장한다.
서빙(0-8 3단계)은 `mlflow.pyfunc.load_model("models:/<이름>@champion")` 한 줄로 이것을 불러 쓴다.

이 파일은 학습 때 `code_paths`로 모델과 함께 저장되므로, 불러오는 쪽에 이 저장소가 없어도 동작한다.
"""

import mlflow.pyfunc
import numpy as np
import pandas as pd


class NorthEastModel(mlflow.pyfunc.PythonModel):
    def __init__(self, north, east, features):
        self.north = north
        self.east = east
        self.features = list(features)

    def predict(self, context, model_input, params=None):
        if isinstance(model_input, pd.DataFrame):
            X = model_input[self.features].to_numpy(dtype=np.float64)
        else:
            X = np.asarray(model_input, dtype=np.float64)
        return pd.DataFrame({"north_m": self.north.predict(X), "east_m": self.east.predict(X)})
