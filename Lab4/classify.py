import pymysql
import json
import operator
import sklearn
import random
import os


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
            sql = "SELECT review.stars as stars, name, neighborhood, address, city, state, postal_code, business.stars as average_stars, review_count" \
                  " FROM review INNER JOIN business ON review.business_id = business.id WHERE user_id = \"{}\"".format(self.user['user_id'])
            cursor.execute(sql)
            results = cursor.fetchall()
            return results


def classify(results):
    # 50/50 distribution of training to testing samples
    training_set = random.sample(results, len(results) / 2)
    test_set = [r for r in results if r not in training_set]
    print len(results), len(training_set), len(test_set)

connector = DatabaseConnector()
connector.user_reviews = connector.get_reviews_from_user()
classify(connector.user_reviews)

