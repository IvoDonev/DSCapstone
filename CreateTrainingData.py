from pymongo import MongoClient
import pandas as pd
import numpy as np
import json
import math

client = MongoClient()
db = client['Capstone']
parcels = db["Parcels"]
training = db["ParcelsWithVariables"]
violations= db["Violations"]
crime = db["Crime"]
threeOneOne = db["ThreeOneOne"]


counter = 0
tot = 0

def GetAllNearParcels(lat, lon):
    nearRes = parcels.aggregate([
    {
        "$geoNear":
        {
            "near": { "type": "Point", "coordinates": [lon, lat] },
            "distanceField": "dist.calculated",
            "maxDistance": 1000, # one kilometer radius around property
            "num": 100, # 30 closest parcels
            "spherical": True
        }
    }])
    return list(nearRes)

def WeightFunction(dist):
    return math.exp(-dist/500)

def GetSecondary(row):

    # show how far we've gotten through the data
    global counter
    counter += 1
    if counter%100 == 0:
        print(counter, " out of ", tot)

    nearest = GetAllNearParcels(row['lat'], row['lon'])
    weights = []
    numCrimes = []
    num311 = []
    numViolations = []

    for nearParcel in nearest:
        dist = nearParcel["dist"]["calculated"]
        weights.append(WeightFunction(dist))
        numCrimes.append(nearParcel["numCrimes"])
        num311.append(nearParcel["num311"])
        numViolations.append(nearParcel["numViolations"])
    
    secondaryNumCrimes = np.average(numCrimes, weights=weights)
    secondaryNum311 = np.average(num311, weights=weights)
    secondaryNumViolations = np.average(numViolations, weights=weights)

    return pd.Series({"secondaryNumCrimes": secondaryNumCrimes,
        "secondaryNumViolations": secondaryNumViolations,
        "secondaryNum311": secondaryNum311})


training.drop() # clear the training data

# # First get all the blighted buildings
# blighted = parcels.aggregate([
# {
#     "$match": { "blighted" : 1 }
# }
# ])

# trainingData = list(blighted)
# numBlighted = len(trainingData)

# # get a random number of non-blighted buildings
# nonBlighted = parcels.aggregate([
# {
#     "$match": { "blighted" : 0 }
# },
# {
#     "$sample": {"size": numBlighted}
# }
# ])


# trainingData.extend(list(nonBlighted))
df = pd.DataFrame(list(parcels.find()))
tot = len(df)

dfFlattened = pd.DataFrame()
dfFlattened['blighted'] = df['blighted']
dfFlattened['state_owned'] = df['properties'].apply(lambda x: 1 if x['owner1']=='DETROIT LAND BANK AUTHORITY' else 0)
dfFlattened['parcelnum'] = df['properties'].apply(lambda x: x['parcelnum'])
dfFlattened['zone'] = df['properties'].apply(lambda x: x['zoning'])

dfFlattened['numViolations'] = df['numViolations']
dfFlattened['numCrimes'] = df['numCrimes']
dfFlattened['num311'] = df['num311']
dfFlattened['nhood_id'] = df['nhood_id']

dfFlattened = pd.concat([dfFlattened, df.apply(GetSecondary, axis=1)], axis=1)

records = json.loads(dfFlattened.T.to_json()).values()
training.insert(records)