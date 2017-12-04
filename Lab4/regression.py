from connect import DatabaseConnector
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction import DictVectorizer
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score

def regression(businesses):
    x = []
    y = []
    for r in businesses:
        row = {
            'review_count': r['count'],
            'neighborhood': r['neighborhood'],
            'address': r['address'],
            'city': r['city'],
            'state': r['state'],
            'category': r['category']
        }
        x.append(row)
        y.append(r['avg_stars'])

    v = DictVectorizer()
    vectorized_features = v.fit_transform(x).toarray()
    feature_names = v.get_feature_names()

    # 50/50 distribution of training to testing samples
    X_train, X_test, y_train, y_test = train_test_split(vectorized_features, y, test_size=0.5, stratify=y, random_state=42)

    regr = linear_model.LinearRegression()
    regr.fit(X_train, y_train)

    y_pred = regr.predict(X_test)

    print("Mean squared error (MSE): %.2f"
         % mean_squared_error(y_test, y_pred))
    print('Variance score: %.2f' % r2_score(y_test, y_pred))
    print('Regression score: %.2f' % (regr.score(X_test,y_test)*100))

connector = DatabaseConnector()
regression(connector.get_businesses())
