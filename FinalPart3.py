"""
Part 3
a.	Using the database with 600,000 tweets, create a new table that
 corresponds to the join of all 3 tables in your database, including records
 without a geo location. This is the equivalent of a materialized view but since
 SQLite does not support MVs, we will use CREATE TABLE AS SELECT (instead of CREATE
 MATERIALIZED VIEW AS SELECT).
"""
import sqlite3
import json
conn = sqlite3.connect('Final_take_home600000.sqlite')
cursor = conn.cursor()
cursor.execute("drop table if exists tweets_users_loc")
cursor.execute("""CREATE TABLE Tweets_Users_Loc AS SELECT * FROM Tweet LEFT JOIN User ON Tweet.
id=User.id LEFT JOIN geo ON Tweet.geo_id=geo.id_geo""")
rows = cursor.execute('SELECT * FROM Tweets_Users_loc').fetchall()
print(rows)

"""
b. Export the contents of your table from 3-a into a new JSON file 
(i.e., create your own JSON file with just the keys you extracted). 
You do not need to replicate the structure of the input and can come 
up with any reasonable keys for each field stored in JSON structure (e.g., 
you can have longitude as “longitude” key when the location is available). 
How does the file size compare to the original input file?
"""

data = []
with open('Tweets_data.json', 'w') as tweets_file:
    for row in rows:
        tweet = {
        'id': row[0],
        'name': row[1],
        'Screen_name': row[2],
        'description': row[3],
        'friends_count': row[4],
        'id_geo': row[5],
        'type': row[6],
        'lat': row[7],
        'long': row[8],
        'id_': row[9],
        'id_str': row[10],
        'text': row[11],
        'source': row[12],
        'truncated': row[13],
        'created_at': row[14],
        'retweet_count': row[15],
        'favorite_count': row[16],
        'favorited': row[17],
        'retweeted': row[18],
        'lang': row[19],
        'possibly_sensitive': row[20],
        'filter_level': row[21],
        'user_id': row[22],
        'unknown': row[23]
        }
        data.append(tweet)
        json.dump(tweet, tweets_file)
        tweet.clear()
conn.close()

"""
c.	Export the contents of your table from 3-a into a .csv (comma separated value) file. 
How does the file size compare to the original input file and to the file in 3-b?
"""

import os
file_path1 = "E:\Masters\Database and analytics\Final_project\Tweets_data.json"
file_size1 = os.path.getsize(file_path1)/1000000
print(f"The size of the JSON file {file_size1} mega bytes.")
file_path2 = 'E:\Masters\Database and analytics\Final_project\Final_take_home600000.sqlite'
file_size2 = os.path.getsize(file_path2)/1000000
print(f"The size of the Sqlite file {file_size2} mega bytes.")
a = file_size1-file_size2
print("The diffrenece between the ", a/1000000)

