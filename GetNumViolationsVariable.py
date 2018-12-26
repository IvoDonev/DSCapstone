from pymongo import MongoClient, InsertOne, DeleteMany, ReplaceOne, UpdateOne
import pandas as pd 

client = MongoClient()
db = client['Capstone']
violations = db["Violations"]
parcels = db["Parcels"]

res = violations.aggregate([
{
	"$group": 
	{
		"_id": "$parcel_id",
		"count": { "$sum" : 1 }
	}
},
{
	"$lookup": 
	{
		"from":"Parcels",
		"localField":"_id",
		"foreignField":"properties.parcelnum",
		"as": "parcel"
	}
},
{
	"$match": {	"parcel": {"$ne": [] } }
},
{
   "$unwind": "$parcel"
},
{
	"$project": { "count": 1 }
}
])

df =  pd.DataFrame(list(res))

# Add a field for each parcel document to indicate whether its blighted or not. 
# Here we initialize them all with 0
parcels.update(
  {},
  {"$set": {"numViolations": 0}},
  multi= True
)


updates = []
for index, row in df.iterrows():
	updates.append(UpdateOne(
		{"properties.parcelnum": row['_id']}, 
		{"$set": { "numViolations": row['count'] }}
	));

parcels.bulk_write(updates);