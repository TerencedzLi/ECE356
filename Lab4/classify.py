import pymysql
import json
import operator


class Classifier:
    def __init__(self):
        self.connection = self.connect_database()
        self.user = {}

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
        if self.user:
            return self.user
        else:
            classifier = Classifier()
            with classifier.connection.cursor() as cursor:
                sql = """
                    SELECT COUNT(user_id) as count, user_id, STDDEV(stars) as std_dev, AVG(stars) as avg_stars
                    FROM review
                    GROUP BY user_id
                    ORDER BY count DESC, std_dev DESC"""
                cursor.execute(sql)
                print cursor.rowcount
                results = cursor.fetchmany(25)
                top_std_dev = sorted(results, key=operator.itemgetter('std_dev'), reverse=True)[0]
                return top_std_dev

                # results = cursor.fetchmany(cursor.rowcount/100)
                # sorted_res = sorted(results, key=operator.itemgetter('std_dev'), reverse=True)
                # print len(sorted_res)
                # sorted_res = sorted_res[:len(sorted_res)/10]
                # for a in sorted_res:
                #     print a['std_dev'], a['count']


