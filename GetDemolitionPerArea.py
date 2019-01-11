from pymongo import MongoClient
import pandas as pd

client = MongoClient()
db = client['Capstone']

res = db["Parcels"].aggregate([
{
	"$match":
	{
		"blighted":1
	}
},
{
	"$group":
	{
		"_id": "$nhood_id",
		"count":{
			"$sum" : 1
		}
	}
}])

df = pd.DataFrame(list(res))
df.to_csv("data/DemolitionPerArea.csv")

# Get coordinates of
res = db["Parcels"].aggregate([
{
	"$match":
	{
		"blighted":1
	}
}
])
df = pd.DataFrame(list(res))
dfBuildings = pd.DataFrame()
dfBuildings['lat'] = df[['lat']]
dfBuildings['lon'] = df[['lon']]
dfBuildings['id'] = df.apply(lambda x: x["properties"]["parcelnum"], axis=1)
dfBuildings['address'] = df.apply(lambda x: x["properties"]["address"], axis=1)
dfBuildings.to_csv("data/DemolishedParcels.csv")