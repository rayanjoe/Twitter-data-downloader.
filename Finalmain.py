"""We will use one full day worth of tweets as our input (there are total of 4.4M tweets in this file,
but we will intentionally use fewer tweets to run this final):
http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt
Execute and time the following tasks with 120,000 tweets and 600,000 tweets:

a.	Use python to download tweets from the web and save to a local text file (not into a database yet,
just to a text file). This is as simple as it sounds, all you need is a for-loop that reads lines from the
web and writes them into a file.
"""


import typing

time_taken_by_parts = {'1.1': [], '1.2': [], '1.3': [], '1.4': []}


def insert_tweet(size):
    import time
    import requests
    target_url = 'http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt'
    start = time.time()
    val = 0
    download_file = f'OneDayOfTweets_{size}.txt'
    with requests.get(url=target_url, stream=True) as response, open(download_file, 'wb') as df:
        for line in response.iter_lines():
            df.write(line)
            df.write(b'\n')
            val += 1
            if val >= size:
                response.close()
                break
    end = time.time()
    a = end - start
    print(f'{val} tweets saved to `{download_file}`')
    print(f'Time take to insert the data into file is', a)
    return a

def table(cursor):
    cursor.execute("""
    CREATE TABLE Tweet(
        id                 INT,
        id_str             VARCHAR(255),
        text               VARCHAR(255),
        source             VARCHAR(255),
        truncated          INT,
        created_at         DATE,
        retweet_count      INT,
        favorite_count     INT,
        favorited          INT,
        retweeted          INT,
        lang               VARCHAR(50),
        possibly_sensitive INT,
        filter_level       VARCHAR(255),
        user_id            INT,
        geo_id             VARCHAR(255),
        FOREIGN KEY (user_id) REFERENCES User (id),
        FOREIGN KEY (geo_id) REFERENCES Geo (id_geo)
    );
    """)
    cursor.execute("""
    CREATE TABLE User
    (
        id INT,
        name VARCHAR(255),
        screen_name VARCHAR(255),
        description VARCHAR(255) NULL ,
        friends_count INT
    );
    """)
    cursor.execute("""
    create table geo
    (
        id_geo varchar(255) primary key,
        type varchar(255),
        latitude varchar(255),
        longitude varchar(255)
    );
    """)
    pass

"""
b. Repeat what you did in part 1-a, but instead of saving tweets to the file, populate the 3-table schema 
that you previously created in SQLite. Be sure to execute commit and verify that the data has been successfully
loaded. Report loaded row counts for each of the 3 tables.
"""


def part1b(size):
    import json
    import sqlite3
    import time
    start = time.time()
    target_url = 'http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt'
    d_name = f'Final_take_home{size}.sqlite'
    con = sqlite3.connect(d_name)
    cursor = con.cursor()
    cursor.execute('DROP TABLE IF EXISTS User;')
    cursor.execute('DROP TABLE IF EXISTS Tweet;')
    cursor.execute('DROP TABLE if EXISTS geo')
    table(cursor)
    import requests
    #download_file = f'OneDayOfTweets_{size}.txt'
    with requests.get(url=target_url, stream=True) as response:
        value = 0

        for line in response.iter_lines():
            general_tweet_dat = json.loads(line.decode('utf-8'))

            def insert_user(user: dict):
                if user is None:
                    return
                sql_user_query = 'INSERT INTO User (id, name, screen_name, description,friends_count) VALUES (?, ?, ?, ?, ?);'
                user_tweet = general_tweet_dat['user']
                id = user_tweet['id']
                name = user_tweet['name']
                screen_name = user_tweet['screen_name']
                description = user_tweet['description']
                friends_count = user_tweet['friends_count']
                vals = (id, name, screen_name, description, friends_count)
                cursor.execute(sql_user_query, vals)
                pass

            insert_user(general_tweet_dat['user'])

            def insert_geo(geo: dict) -> typing.Optional[str]:
                if geo is None:
                    return None
                id_geo = str(geo['coordinates'][0]) + "_" + str(geo['coordinates'][1])
                if cursor.execute('select * from Geo where id_geo = ?', (id_geo,)).fetchone() is not None:
                    return
                sql = 'INSERT INTO geo (id_geo, type, latitude, longitude) VALUES (?, ?, ?, ?);'
                vals = (
                    id_geo,
                    geo['type'],
                    geo['coordinates'][0],
                    geo['coordinates'][1],
                )
                cursor.execute(sql, vals)
                return id_geo
                pass

            id_geo = insert_geo(general_tweet_dat['geo'])

            def insert_tweet(user_id):
                sql_tweet_query = 'INSERT INTO Tweet (id, id_str, text, source, truncated, created_at, retweet_count,favorite_count, favorited,retweeted, lang, possibly_sensitive, filter_level, user_id, geo_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'
                twet = (
                    general_tweet_dat['id'],
                    general_tweet_dat['id_str'],
                    general_tweet_dat['text'],
                    general_tweet_dat['source'],
                    general_tweet_dat['truncated'],
                    general_tweet_dat['created_at'],
                    general_tweet_dat['retweet_count'],
                    general_tweet_dat['favorite_count'],
                    general_tweet_dat['favorited'],
                    general_tweet_dat['retweeted'],
                    general_tweet_dat['lang'],
                    general_tweet_dat['possibly_sensitive'],
                    general_tweet_dat['filter_level'],
                    user_id,
                    id_geo
                )
                cursor.execute(sql_tweet_query, twet)
                pass

            insert_tweet(general_tweet_dat['user']['id'])
            value += 1
            if value >= size:
                response.close()
                break

        print(f'{value} tweets saved to `{d_name}`')
        pass
    con.commit()


    def count_rows(table: str):
        sql = f"select count(*) from {table};"
        count = cursor.execute(sql).fetchone()
        return count[0]
        pass

    print(f"\n{count_rows('Tweet')} rows loaded to Tweet table")
    print(f"{count_rows('User')} rows loaded to User table")
    print(f"{count_rows('Geo')} rows loaded to Geo table\n")
    end = time.time() - start
    print(f'Time take to insert the data into file is', end)
    con.close()
    return end




"""
c.	Use your locally saved tweet file to repeat the database population step from part-c.
 That is, load the tweets into the 3-table database using your saved file with tweets.
  This is the same code as in 1-b, but reading tweets from your file, not from the web.
"""


def part1c(size):
    import json
    import sqlite3
    import time
    start = time.time()
    target_url = 'http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt'
    d_name = f'Final_take_home{size}.sqlite'
    con = sqlite3.connect(d_name)
    cursor = con.cursor()
    cursor.execute('DROP TABLE IF EXISTS User;')
    cursor.execute('DROP TABLE IF EXISTS Tweet;')
    cursor.execute('DROP TABLE if EXISTS geo')
    table(cursor)
    import requests
    download_file = f'OneDayOfTweets_{size}.txt'
    with open(download_file, 'rb') as df:
        value = 0

        for line in df:
            general_tweet_dat = json.loads(line.decode('utf-8'))

            def insert_user(user: dict):
                if user is None:
                    return
                sql_user_query = 'INSERT INTO User (id, name, screen_name, description,friends_count) VALUES (?, ?, ?, ?, ?);'
                user_tweet = general_tweet_dat['user']
                id = user_tweet['id']
                name = user_tweet['name']
                screen_name = user_tweet['screen_name']
                description = user_tweet['description']
                friends_count = user_tweet['friends_count']
                vals = (id, name, screen_name, description, friends_count)
                cursor.execute(sql_user_query, vals)

            insert_user(general_tweet_dat['user'])

            def insert_geo(geo: dict) -> typing.Optional[str]:
                if geo is None:
                    return None
                id_geo = str(geo['coordinates'][0]) + "_" + str(geo['coordinates'][1])
                if cursor.execute('select * from Geo where id_geo = ?', (id_geo,)).fetchone() is not None:
                    return
                sql = 'INSERT INTO geo (id_geo, type, latitude, longitude) VALUES (?, ?, ?, ?);'
                vals = (
                    id_geo,
                    geo['type'],
                    geo['coordinates'][0],
                    geo['coordinates'][1],
                )
                cursor.execute(sql, vals)
                return id_geo
                pass

            id_geo = insert_geo(general_tweet_dat['geo'])

            def insert_tweet(user_id):
                sql_tweet_query = 'INSERT INTO Tweet (id, id_str, text, source, truncated, created_at, retweet_count,favorite_count, favorited,retweeted, lang, possibly_sensitive, filter_level, user_id, geo_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'
                twet = (
                    general_tweet_dat['id'],
                    general_tweet_dat['id_str'],
                    general_tweet_dat['text'],
                    general_tweet_dat['source'],
                    general_tweet_dat['truncated'],
                    general_tweet_dat['created_at'],
                    general_tweet_dat['retweet_count'],
                    general_tweet_dat['favorite_count'],
                    general_tweet_dat['favorited'],
                    general_tweet_dat['retweeted'],
                    general_tweet_dat['lang'],
                    general_tweet_dat['possibly_sensitive'],
                    general_tweet_dat['filter_level'],
                    user_id,
                    id_geo
                )
                cursor.execute(sql_tweet_query, twet)
                pass

            insert_tweet(general_tweet_dat['user']['id'])
            value += 1
            if value >= size:
                break

        print(f'{value} tweets saved to `{d_name}`')
        pass
    con.commit()

    def count_rows(table: str):
        sql = f"select count(*) from {table};"
        count = cursor.execute(sql).fetchone()
        return count[0]
        pass

    print(f"\n{count_rows('Tweet')} rows loaded to Tweet table")
    print(f"{count_rows('User')} rows loaded to User table")
    print(f"{count_rows('Geo')} rows loaded to Geo table\n")
    end = time.time() - start
    print(f'Time take to insert the data into file is', end)
    con.close()
    return end




"""
d.	Repeat the same step with a batching size of 6000 (i.e. 
by inserting 6000 rows at a time with executemany instead of doing individual inserts). 
Since many of the tweets are missing a Geo location, its fine for the batches of Geo inserts to 
be smaller than 6000. 
"""

def part1d(size):
    import json
    import sqlite3
    import time
    start = time.time()
    target_url = 'http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt'
    d_name = f'Final_take_home{size}.sqlite'
    con = sqlite3.connect(d_name)
    cursor = con.cursor()
    cursor.execute('DROP TABLE IF EXISTS User;')
    cursor.execute('DROP TABLE IF EXISTS Tweet;')
    cursor.execute('DROP TABLE if EXISTS geo')
    table(cursor)
    import requests
    download_file = f'OneDayOfTweets_{size}.txt'
    with open(download_file, 'rb') as df:
        value = 0
        user_tweet_dat = []
        geo_tweet_dat = []
        tweet_tweet_dat = []
        geo_dup = []
        def batch():
            sql_user_query = 'INSERT INTO User (id, name, screen_name, description,friends_count) VALUES (?, ?, ?, ?, ?);'
            cursor.executemany(sql_user_query, user_tweet_dat)
            sql_geo_query = 'INSERT INTO geo (id_geo, type, latitude, longitude) VALUES (?, ?, ?, ?);'
            cursor.executemany(sql_geo_query, geo_tweet_dat)
            sql_tweet_query = 'INSERT INTO Tweet (id, id_str, text, source, truncated, created_at, retweet_count,favorite_count, favorited,retweeted, lang, possibly_sensitive, filter_level, user_id, geo_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'
            cursor.executemany(sql_tweet_query, tweet_tweet_dat)
            user_tweet_dat.clear()
            geo_tweet_dat.clear()
            tweet_tweet_dat.clear()
        for line in df:
            general_tweet_dat = json.loads(line.decode('utf-8'))

            def insert_user(user: dict):
                if user is None:
                    return
                user_tweet = general_tweet_dat['user']
                id = user_tweet['id']
                name = user_tweet['name']
                screen_name = user_tweet['screen_name']
                description = user_tweet['description']
                friends_count = user_tweet['friends_count']
                vals = (id, name, screen_name, description, friends_count)
                user_tweet_dat.append(vals)


            insert_user(general_tweet_dat['user'])

            def insert_geo(geo: dict) -> typing.Optional[str]:
                if geo is None:
                    return None
                id_geo = str(geo['coordinates'][0]) + "_" + str(geo['coordinates'][1])
                if id_geo in geo_dup:
                    return
                vals = (
                    id_geo,
                    geo['type'],
                    geo['coordinates'][0],
                    geo['coordinates'][1],
                )
                geo_tweet_dat.append(vals)
                geo_dup.append(id_geo)
                return id_geo
                pass

            id_geo = insert_geo(general_tweet_dat['geo'])

            def insert_tweet(user_id):
                twet = (
                    general_tweet_dat['id'],
                    general_tweet_dat['id_str'],
                    general_tweet_dat['text'],
                    general_tweet_dat['source'],
                    general_tweet_dat['truncated'],
                    general_tweet_dat['created_at'],
                    general_tweet_dat['retweet_count'],
                    general_tweet_dat['favorite_count'],
                    general_tweet_dat['favorited'],
                    general_tweet_dat['retweeted'],
                    general_tweet_dat['lang'],
                    general_tweet_dat['possibly_sensitive'],
                    general_tweet_dat['filter_level'],
                    user_id,
                    id_geo
                )
                tweet_tweet_dat.append(twet)
                pass

            insert_tweet(general_tweet_dat['user']['id'])
            value += 1

            if value >= 6000:
                batch()
                pass

            if value >= size:
                break

        print(f'{value} tweets saved to `{d_name}`')
        pass
    con.commit()

    def count_rows(table: str):
        sql = f"select count(*) from {table};"
        count = cursor.execute(sql).fetchone()
        return count[0]
        pass

    print(f"\n{count_rows('Tweet')} rows loaded to Tweet table")
    print(f"{count_rows('User')} rows loaded to User table")
    print(f"{count_rows('Geo')} rows loaded to Geo table\n")
    end = time.time() - start
    print(f'Time take to insert the data into file is', end)
    con.close()
    return end





"""
e.	Plot the resulting runtimes (# of tweets versus runtimes) using
 matplotlib for 1-a, 1-b, 1-c, and 1-d. How does the runtime compare?
"""

def part1e(s):
    import matplotlib.pyplot as plt
    import pandas as pd
    df = pd.DataFrame(s , index=[120000, 600000])
    df.plot.bar()
    plt.xlabel('Number of Tweets')
    plt.ylabel('Runtime (seconds)')
    plt.show()
print(time_taken_by_parts)

a = insert_tweet(120000)
time_taken_by_parts['1.1'].append(a)
b = insert_tweet(600000)
time_taken_by_parts['1.1'].append(a)

b = part1b(120000)
time_taken_by_parts['1.2'].append(b)
part1b(600000)
time_taken_by_parts['1.2'].append(b)

b = part1c(120000)
time_taken_by_parts['1.3'].append(b)
b = part1c(600000)
time_taken_by_parts['1.3'].append(b)

b = part1d(120000)
time_taken_by_parts['1.4'].append(b)
part1d(600000)
time_taken_by_parts['1.4'].append(b)

part1e(time_taken_by_parts)


