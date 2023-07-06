import sqlite3
import time
import pandas as pd

"""
a.	finding the average longitude and latitude value for each user ID."""


def part2a(size):
    d_name = f'Final_take_home{size}.sqlite'
    con = sqlite3.connect(d_name)
    cursor = con.cursor()
    start = time.time()
    results = cursor.execute(
        """
        select user_id, avg(longitude), avg(latitude)
        from Tweet, Geo
        where Tweet.geo_id = Geo.id_geo group by user_id order by user_id;
        """
    ).fetchall()
    con.close()
    end = time.time() - start
    print(f'Time take to insert the data into file is', end)
    df = {'user_id': [], 'Average_Latitude': [], 'Average_Longitude': []}
    for row in results[0:10]:
        df['user_id'].append(row[0])
        df['Average_Latitude'].append(row[1])
        df['Average_Longitude'].append(row[2])
        pass

    df = pd.DataFrame(df)
    print(df)
    return df


part2a(120000)


print("##########################################################")
"""
b.	Re-executing the SQL query in Finalpart.py "A", 5 times and 20 times and measure the total runtime 
"""


def part2b(number):
    d_name = f'Final_take_home120000.sqlite'
    con = sqlite3.connect(d_name)
    cursor = con.cursor()
    start = time.time()
    for i in range(number):
        part2a(120000)
        pass
    con.close()
    end = time.time() - start
    print(f'Time take is', end)
    return end

iteration1 = part2b(1)
iteration5 = part2b(5)
iteration20 = part2b(20)

if iteration1 * 5 >= iteration5 and iteration1 * 20 >= iteration20:
    print("it is linear")
else:
    print("It's not a linear")
print("##########################################################")
p12_1time = iteration1
p12_5time = iteration5
p12_20time = iteration20


"""
c.	Write the equivalent of the "B" query in python (without using SQL) 
by reading it from the file with 600,000 tweets.
"""

import json
import itertools

data = {}

def part2c():
    with open('OneDayOfTweets_600000.txt', 'rb') as file:
        for line in file:
            tweet = json.loads(line.decode('utf-8'))
            user_id = tweet['user']['id']
            if tweet['geo'] is not None:
                coordinates = tweet['geo']['coordinates']
                latitude = coordinates[0]
                longitude = coordinates[1]
                if user_id in data.keys():
                    table = data[user_id]
                    if tweet['geo']['coordinates'][0] is None:
                        longitude = 0
                        latitude = 0
                    table[0] += longitude
                    table[1] += latitude
                    table[2] += 1
                    data[user_id] = table
                    pass
                else:
                    table = []
                    if tweet['geo']['coordinates'][1] is None:
                        latitude = 0
                        longitude = 0
                        pass
                    table.append(longitude)
                    table.append(latitude)
                    table.append(1)
                    data[user_id] = table
                    pass
                pass
            pass
        sorted_dict = dict(itertools.islice(dict(sorted(data.items())).items(), 10))
        df = {'user_id': [], 'avg(longitude)': [], 'avg(latitude)': []}
        for user_id in sorted_dict:
            table = data[user_id]
            avg_longitude = table[0] / table[2]
            avg_latitude = table[1] / table[2]

            df['user_id'].append(user_id)
            df['avg(longitude)'].append(avg_longitude)
            df['avg(latitude)'].append(avg_latitude)
            pass

        df = pd.DataFrame(df)
        print(df)
        return df
        pass
part2c()
print("##########################################################")

"""
d.	Re-executing the query in part "C", 5 times and 20 times and measure the total runtime.(To test the runtime weather it is linear or not!)
"""


def part2d(number):
    d_name = f'Final_take_home120000.sqlite'
    con = sqlite3.connect(d_name)
    cursor = con.cursor()
    start = time.time()
    for i in range(number):
        part2c()
    end = time.time() - start
    print(f'Time take is', end)
    return end


iteration1 = part2d(1)
iteration5 = part2d(5)
iteration20 = part2d(20)

if iteration1 * 5 >= iteration5 and iteration1 * 20 >= iteration20:
    print("it is linear")
else:
    print("It''s not a linear")
    pass
p14_1time = iteration1
p14_5time = iteration5
p14_20time = iteration20
print("##########################################################")

"""
e.	Write the equivalent of the FinalPart2a.py "A" query in python by using regular expressions instead of 
json.loads().
"""
import itertools
import re
import time
import pandas as pd
def part2e():
    download_file = f'OneDayOfTweets_600000.txt'
    data_dict = dict()
    with open(download_file, 'rb') as fp:
        tweet_count = 0
        count = 0
        for line in fp:
            if tweet_count >= 250_000:
                break
                pass
            tweet_count += 1
            tweet = line.decode('utf-8')
            tweet = str(tweet)
            m = re.search(r'(?<="user":{"id":)\d+', tweet)
            user_id = m.group(0)
            user_id = int(user_id)
            geo_data = None
            m = re.search(r'(?<="geo":)\w+', tweet)
            if m and m.group(0) == 'null':
                geo_data = False
                pass
            latitude = None
            longitude = None
            if geo_data is None:
                m = re.search(r'"geo":\{(.*?)\}', tweet)
                temp = m.group(0)
                m = re.search(r'\[(.*?)\]', temp)
                temp = m.group(0)
                coordinates = temp.rstrip(']').lstrip('[').split(',')
                latitude = float(coordinates[0])
                longitude = float(coordinates[1])
                if user_id in data_dict.keys():
                    table = data_dict[user_id]
                    if coordinates[0] is None:
                        longitude = 0
                        latitude = 0
                    table[0] += longitude
                    table[1] += latitude
                    table[2] += 1
                    data_dict[user_id] = table
                    pass
                else:
                    table = [
                    ]
                    if coordinates[1] is None:
                        latitude = 0
                        longitude = 0
                        pass
                    table.append(longitude)
                    table.append(latitude)
                    table.append(1)
                    data_dict[user_id] = table
                    pass
                pass
            pass
        pass
    sorted_dict = dict(itertools.islice(dict(sorted(data_dict.items())).items(), 10))
    df = {'user_id': [], 'avg(longitude)': [], 'avg(latitude)': []}
    for user_id in sorted_dict:
        table = data_dict[user_id]
        avg_longitude = table[0] / table[2]
        avg_latitude = table[1] / table[2]
        df['user_id'].append(user_id)
        df['avg(longitude)'].append(avg_longitude)
        df['avg(latitude)'].append(avg_latitude)
        pass
    df = pd.DataFrame(df)
    print(df)
    pass
part2e()
print("##########################################################")

"""
f.	Re-execute the query in part 2-e 5 times and 20 times and measure the total runtime. 
Does the runtime scale linearly? 
"""

def part2f(number):
    d_name = f'Final_take_home120000.sqlite'
    con = sqlite3.connect(d_name)
    cursor = con.cursor()
    start = time.time()
    for i in range(number):
        part2e()
    end = time.time() - start
    print(f'Time take is', end)
    return end


iteration1 = part2f(1)
iteration5 = part2f(5)
iteration20 = part2f(20)

if iteration1 * 5 >= iteration5 and iteration1 * 20 >= iteration20:
    print("it is linear")

else:
    print("It''s not a linear")
    pass
print("##########################################################")

p16_1time = iteration1
p16_5time = iteration5
p16_20time = iteration20

print("Question 2a iteration time are")
print("1 time " , p12_1time)
print("5 time " , p12_5time)
print("20 time " , p12_20time)

if p12_1time * 5 >= p12_5time and p12_1time * 20 >= p12_20time:
    print("it is linear")
else:
    print("It's not a linear")

print("Question 2c iteration time are")
print("1 time " , p14_1time)
print("5 time " , p14_5time)
print("20 time " , p14_20time)
if p14_1time * 5 >= p14_5time and p14_1time * 20 >= p14_20time:
    print("it is linear")
else:
    print("It's not a linear")

print("Question 2e iteration time are")
print("1 time " , p16_1time)
print("5 time " , p16_5time)
print("20 time " , p16_20time)
if p16_1time * 5 >= p16_5time and p16_1time * 20 >= p16_20time:
    print("it is linear")
else:
    print("It's not a linear")