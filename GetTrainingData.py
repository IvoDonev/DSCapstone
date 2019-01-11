from pymongo import MongoClient
import pandas as pd

client = MongoClient()
db = client['Capstone']
parcels = db["ParcelsWithVariables"]

def TransformData(Xin):
	X = Xin
	# Do a one-hot encoding of the nhood ids into seperate variables
	# to prevent them being treated numerically when they are categorical
	nhood = pd.get_dummies(X['nhood_id'], prefix='nhood')
	X = X.drop('nhood_id', axis=1)
	X = pd.concat([X, nhood], axis=1)

	# Do the same one-hot-enconding for the zoning categeories
	zone = pd.get_dummies(X['zone'], prefix='zone')
	X = X.drop('zone', axis=1)
	X = pd.concat([X, zone], axis=1)

	# remove the id and parcel num from training data
	X = X.drop('parcelnum', axis=1)
	X = X.drop('_id', axis=1)
	return X

def GetData():

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

	df = pd.DataFrame(trainingData)

	Y = df[['blighted']].values.ravel()
	X = df.drop('blighted', axis=1)
	return (X, Y)

def GetAllData():
	df = pd.DataFrame(list(parcels.find()))
	Y = df[['blighted']].values.ravel()
	X = df.drop('blighted', axis=1)
	return (X, Y)