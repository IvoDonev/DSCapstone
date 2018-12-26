from pymongo import MongoClient
import pandas as pd 

client = MongoClient()
db = client['Capstone']

res = db["Crime"].aggregate([
{
	"$group": 
	{
		"_id": "$nhood_id",
		"count":{
			"$sum" : 1 
		}
	}
}
{
	"$lookup": {
		"from":"Areas",
		"localField":"_id",
		"foreignField":"properties.target_fid",
		"as": "area"
	}
},
{
	"$match": {
		"area": {"$ne": []}
	}
},
{
   "$unwind": "$area"
},
{
	"$project": {
		"count": 1,
		"name": "$area.properties.new_nhood"
	}
}
])

df =  pd.DataFrame(list(res))
df.to_csv("data/CrimePerArea.csv")