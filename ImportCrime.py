import pandas as pd
import json
from pymongo import MongoClient
import QueryBuildings as query

client = MongoClient()
db = client['Capstone']
crime = db['Crime']

df = pd.read_csv("data/detroit-crime.csv", quotechar='"', low_memory=False)

c = 0
tot = len(df)
for index, row in df.iterrows():

	# show how far we've gotten through the data
	c += 1
	if c%100 == 0:
		print(c, " out of ", tot)

	nhood_id = query.GetArea(row['LAT'], row['LON'])
	if nhood_id is None:
		nhood_id = 0
	df.at[index, 'nhood_id'] = nhood_id;

	parcel_id = query.GetBuilding(row['LAT'], row['LON'], row['ADDRESS'])
	if parcel_id is None:
		parcel_id = 0
	df.at[index, 'parcel_id'] = parcel_id


records = json.loads(df.T.to_json()).values()

crime.insert(records)
