import QueryBuildings as query
import geopandas as gpd
import pandas as pd

df = gpd.read_file("data/Detroit Neighborhoods.geojson")
df = df[['nhood_name', 'nhood_num']]
#df["nhood_num"] = pd.to_numeric(df["nhood_num"])
df["numCrimes"] = 0

counts = {}
for index, row in df.iterrows():
	counts[row['nhood_num']] = 0;

c = 0
dfCrime = pd.read_csv("data/detroit-crime.csv", quotechar='"', low_memory=False)
tot = len(dfCrime)

for index, row in dfCrime.iterrows():

	# show how far we've gotten through the data
 	c += 1
 	if c%100 == 0:
		print(c, " out of ", tot)

	nhood_id = query.GetArea(row['LAT'], row['LON'])
	if not nhood_id is None:
		counts[nhood_id] += 1
	
df['numCrimes'] = df.apply(lambda x: counts[x['nhood_num']], axis=1)

df.to_csv('data/CrimesPerArea.csv')