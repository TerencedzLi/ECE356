import pandas as pd
from connect import DatabaseConnector
from sklearn import metrics
from sklearn import tree
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction import DictVectorizer


def classify(results):
    for r in results:
        r['name'] = unicode(r['name'], "utf-8", errors='replace')
        r['address'] = unicode(r['address'], "utf-8", errors='replace')

    vec = DictVectorizer(sparse=False)

    df = pd.DataFrame(results)
    target = 'stars'
    y = df[target]

    for r in results:
        del r['stars']
    x_results = vec.fit_transform(results)
    x = pd.DataFrame(x_results)

    # 50/50 distribution of training to testing samples
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.5, stratify=y, random_state=123456)

    clf = tree.DecisionTreeClassifier(max_depth=30)
    clf.fit(x_train, y_train)

    preds = clf.predict(x_test)
    accuracy = metrics.accuracy_score(y_test, preds)
    report = metrics.classification_report(y_test, preds)
    matrix = metrics.confusion_matrix(y_test, preds)
    print(matrix)

    tree.export_graphviz(clf, out_file='tree.dot')
    # Convert to jpg using dot -Tjpg tree.dot > tree.jpg

    print report
    print "Accuracy: {}".format(accuracy)

connector = DatabaseConnector()
connector.user_reviews = connector.get_reviews_from_user()
classify(connector.user_reviews)