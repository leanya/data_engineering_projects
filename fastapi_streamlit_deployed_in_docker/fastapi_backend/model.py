from sklearn.datasets import load_iris
from sklearn.naive_bayes import GaussianNB
from sklearn.model_selection import train_test_split

def dataset():
    iris = load_iris()
    X = iris.data
    y = iris.target

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.33, random_state=42)
    return X_train, y_train, X_test, y_test

def get_test_dataset():
    _, _, X_test, y_test = dataset()
    return X_test, y_test

def train_model():
    X, y, _, _ = dataset()
    clf = GaussianNB()
    clf.fit(X,y)
    return clf