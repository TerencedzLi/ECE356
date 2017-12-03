import json
import operator
import os
import pandas as pd
import pymysql
from sklearn import metrics
from sklearn import tree
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction import DictVectorizer



class DatabaseConnector:
    def __init__(self):
        self.connection = self.connect_database()
        self.user = self.get_user()
        self.user_reviews = {}

    def connect_database(self):
        # Must have a credentials file with user and password stored in a JSON object

        credentials = self.load_credentials()

        return pymysql.connect(host='localhost',
                               user=credentials['user'],
                               password=credentials['password'],
                               db='yelp_db',
                               connect_timeout=600000,
                               read_timeout=600000,
                               max_allowed_packet=256 * 1024 * 1024,
                               cursorclass=pymysql.cursors.DictCursor)

    def load_credentials(self):
        with open('../../credentials.txt') as infile:
            return json.load(infile)

    def get_user(self):
        # If user info is already in a file
        txt_file = 'user.txt'
        if os.path.exists(txt_file) and os.path.getsize(txt_file) > 0:
            with open(txt_file) as infile:
                return json.load(infile)

        with self.connection.cursor() as cursor:
            sql = """
                SELECT COUNT(user_id) as count, user_id, STDDEV(stars) as std_dev, AVG(stars) as avg_stars
                FROM review
                GROUP BY user_id
                ORDER BY count DESC, std_dev DESC"""
            cursor.execute(sql)
            results = cursor.fetchmany(25)
            top_std_dev = sorted(results, key=operator.itemgetter('std_dev'), reverse=True)[0]

            # Convert avg_stars from Decimal object to float to enable JSON serialization
            top_std_dev['avg_stars'] = float(top_std_dev['avg_stars'])

            # Load user information to file to make testing quicker
            with open(txt_file, 'w') as outfile:
                json.dump(top_std_dev, outfile)
            return top_std_dev

    def get_reviews_from_user(self):
        with self.connection.cursor() as cursor:
            sql = "SELECT review.stars as stars, name, address, city, postal_code, business.stars as average_stars, review_count, category" \
                  " FROM review INNER JOIN business ON review.business_id = business.id INNER JOIN category ON category.business_id = review.business_id " \
                  "WHERE user_id = \"{}\"".format(self.user['user_id'])
            cursor.execute(sql)
            results = cursor.fetchall()
            return results


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
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.1, stratify=y, random_state=123456)

    clf = tree.DecisionTreeClassifier(max_depth=25)
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

