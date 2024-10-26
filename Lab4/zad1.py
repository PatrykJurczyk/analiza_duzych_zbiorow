import dask.dataframe as dd
from dask_ml.datasets import make_classification
from dask_ml.linear_model import LogisticRegression
from dask.distributed import Client

# dask-scheduler
# dask-worker tcp://localhost:8786

if __name__ == '__main__':
    client = Client("tcp://192.168.0.215:8786")

    n_samples = 50_000_000
    n_features = 100
    X, y = make_classification(n_samples=n_samples, n_features=n_features, chunks=5_000_000, random_state=42)

    model = LogisticRegression(max_iter=500)

    model.fit(X, y)

    print("Trening ukończony.")
