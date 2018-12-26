from pymongo import MongoClient
import pandas as pd 
import json

client = MongoClient()
db = client['Capstone']
parcels = db["Parcels"]
training = db["Training"]

# First get all the blighted buildings
blighted = parcels.aggregate([
{
	"$match": { "blighted" : 1 }
}
])

trainingData = list(blighted)
numBlighted = len(trainingData)

# get a random number of non-blighted buildings
nonBlighted = parcels.aggregate([
{
	"$match": { "blighted" : 0 }
},
{
	"$sample": {"size": numBlighted}
}
])

trainingData.extend(list(nonBlighted))
df = pd.DataFrame(list(trainingData))

dfFlattened = df[['blighted']]
dfFlattened['parcelnum'] = df['properties'].apply(lambda x: x['parcelnum'])
records = json.loads(dfFlattened.T.to_json()).values()
training.insert(records)

