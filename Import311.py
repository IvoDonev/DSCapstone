import pandas as pd
import json
from pymongo import MongoClient
import QueryBuildings as query

client = MongoClient()
db = client['Capstone']
ThreeOneOne = db['ThreeOneOne']

df = pd.read_csv("data/detroit-311.csv", quotechar='"', low_memory=False)

c = 0
tot = len(df)
for index, row in df.iterrows():

	#print(row)
	#exit()

	# show how far we've gotten through the data
	c += 1
	if c%100 == 0:
		print(c, " out of ", tot)

	nhood_id = query.GetArea(row['lat'], row['lng'])
	if nhood_id is None:
		nhood_id = 0
	df.at[index, 'nhood_id'] = nhood_id;

	parcel_id = query.GetBuilding(row['lat'], row['lng'])
	if parcel_id is None:
		parcel_id = 0
	df.at[index, 'parcel_id'] = parcel_id


records = json.loads(df.T.to_json()).values()

ThreeOneOne.insert(records)
