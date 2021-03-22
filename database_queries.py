# this file extracts parsed data from the database on my home machine and saves it as a serialized Pandas dataframe.

import pymongo
import pandas as pd


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["short-story-parser"]
collection = db["weeks"]

cursor = collection.find({}, {"_id": 0, "storiesUrls": 1})

stories_urls = []
for document in cursor:
    stories_urls += document["storiesUrls"].split(',')

collection = db["stories"]
cursor = collection.find({}, {"_id": 0})

stories = []
for document in cursor:
    stories.append(document)

df = pd.DataFrame(stories)

# append list of urls as a column
df["url"] = stories_urls

print(df)
print(df.columns.values)

df.to_pickle("./data/stories_pandas_df.pkl")
