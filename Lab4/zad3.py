from dask_ml.model_selection import IncrementalSearchCV
from dask_ml.datasets import make_classification
from sklearn.linear_model import SGDClassifier
from dask.distributed import Client
import numpy as np


# dask-scheduler
# dask-worker tcp://localhost:8786

client = Client("tcp://192.168.0.215:8786")

n_samples = 10_000
X, y = make_classification(n_samples=n_samples, n_features=20, chunks=1_000_000, random_state=0)

param_grid = {
    'penalty': ['l2', 'l1'],
    'alpha': [0.0001, 0.001, 0.01, 0.1, 1]
}

model = SGDClassifier(loss='log_loss', random_state=0)
search = IncrementalSearchCV(model, param_grid)
search.fit(X, y, classes=np.unique(y))

print("Najlepsze hiperparametry:", search.best_params_) # {'penalty': 'l2', 'alpha': 0.0001}