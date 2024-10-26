import joblib
import dask.dataframe as dd
from dask_ml.datasets import make_classification
from dask_ml.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix

n_samples = 10_000
X, y = make_classification(n_samples=n_samples, n_features=20, chunks=1_000_000, random_state=0)

model = LogisticRegression()
for chunk in range(0, n_samples, 1_000_000):
    X_chunk = X[chunk:chunk + 1_000_000]
    y_chunk = y[chunk:chunk + 1_000_000]
    model.fit(X_chunk, y_chunk)

joblib.dump(model, 'model_incremental.pkl')

loaded_model = joblib.load('model_incremental.pkl')

X_test, y_test = make_classification(n_samples=1_000_000, n_features=20, chunks=1_000_000, random_state=1)

y_pred = loaded_model.predict(X_test)

cm = confusion_matrix(y_test.compute(), y_pred.compute())
print("Macierz konfuzji:\n", cm)
