import re
import pandas as pd
import json
from pymongo import MongoClient
import QueryBuildings as query

client = MongoClient()
db = client['Capstone']
Violations = db['Violations']

df = pd.read_csv("data/detroit-blight-violations.csv", quotechar='"', low_memory=False)

c = 0
tot = len(df)
for index, row in df.iterrows():

	# show how far we've gotten through the data
	c += 1
	if c%100 == 0:
		print(c, " out of ", tot)

	loc = row['ViolationAddress']
	if (type(loc) is not str):
		df.at[index, 'nhood_id'] = 0
		df.at[index, 'parcel_id'] = 0
		continue

	lines = loc.splitlines()
	address = lines[0]
	lat = -999.9; # set to something unreasonable to get thrown out if we don't find it
	lng = -999.9;
	if (len(lines) > 2):
		match = re.search('(\()( ?[-0-9]*\.?[0-9]+.)(,)( ?[-0-9]*\.?[0-9]+.)(\))', lines[2])
		if not match is None:
			lat = float(match.group(2))
			lng = float(match.group(4))

	nhood_id = query.GetArea(lat, lng)
	if nhood_id is None:
		nhood_id = 0
	df.at[index, 'nhood_id'] = nhood_id;

	parcel_id = query.GetBuilding(lat, lng, address)
	if parcel_id is None:
		parcel_id = 0
	df.at[index, 'parcel_id'] = parcel_id


records = json.loads(df.T.to_json()).values()

Violations.insert(records)
