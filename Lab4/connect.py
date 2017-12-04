import json
import operator
import os

import pymysql


class DatabaseConnector:
    def __init__(self):
        self.connection = self.connect_database()
        self.user = self.get_user()
        self.user_reviews = {}
        self.businesses = self.get_businesses()

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

    def get_businesses(self):
        # If user info is already in a file
        txt_file = 'businesses.txt'
        if os.path.exists(txt_file) and os.path.getsize(txt_file) > 0:
            with open(txt_file) as infile:
                return json.load(infile)
        with self.connection.cursor() as cursor:
            sql = """
                SELECT *
                FROM business
                INNER JOIN
                    (SELECT business_id, count(business_id) as count, avg(stars) as avg_stars, stddev(stars) as std_dev
                    FROM review
                    GROUP BY business_id
                    ORDER BY count DESC, std_dev DESC
                    LIMIT 2000) as t
                ON business.id = t.business_id
                """
            cursor.execute(sql)
            results = cursor.fetchmany(2000)
            # Convert avg_stars from Decimal object to float to enable JSON serialization
            for r in results:
                r['avg_stars'] = float(r['avg_stars'])
                r['city'] = unicode(r['city'], "utf-8", errors='replace')
                r['name'] = unicode(r['name'], "utf-8", errors='replace')
            with open(txt_file, 'w') as outfile:
                json.dump(results, outfile)
            return results

    def get_reviews_from_user(self):
        with self.connection.cursor() as cursor:
            sql = "SELECT review.stars as stars, name, address, city, postal_code, business.stars as average_stars, review_count, category" \
                  " FROM review INNER JOIN business ON review.business_id = business.id INNER JOIN category ON category.business_id = review.business_id " \
                  "WHERE user_id = \"{}\"".format(self.user['user_id'])
            cursor.execute(sql)
            results = cursor.fetchall()
            return results

