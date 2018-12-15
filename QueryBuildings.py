from pymongo import MongoClient
import csv
import pandas as pd 
import math

client = MongoClient()
db = client['Capstone']

city = db['City']
neighborhoods = db['Areas']
parcels = db['Parcels']


def isLatLonValid(lat, lon):
	# Check that lat lon is number and is not NaN
	if math.isnan(lat) or math.isnan(lon):
		return False

	if not isinstance(lat, (int, float, complex)) or not isinstance(lon, (int, float, complex)):
		return False

	# Check feasible lat lon values
	if lat > 360.0 or lat < -360.0:
		return False
	if lon > 360.0 or lon < -360.0:
		return False
	return True

def isInDetroit(lat, lon):
	results = city.find_one({'geometry' : {'$geoIntersects': {'$geometry': {'type':'Point', 'coordinates':[lon, lat] } } } })
	return not (results is None)

def GetArea(lat, lon):

	if not isLatLonValid(lat, lon):
		return None
	if not isInDetroit(lat, lon):
		return None

	results = neighborhoods.find_one({'geometry' : {'$geoIntersects': {'$geometry': {'type':'Point', 'coordinates':[lon, lat] } } } })
	if results is None:
		return None
	else:
		return results["properties"]["target_fid"]

def GetBuilding(lat, lon, address = None):
	results = None

	if not isLatLonValid(lat, lon):
		return None

	# Check coordinates are inside Detroit
	if not isInDetroit(lat, lon):
		return None

	# First try and find by address if it is available
	if not address is None:
		results = parcels.find_one({'properties.address': address.lstrip('0')})

	# Then try and find if it falls within a parcel
	if results is None:
		results = parcels.find_one({'geometry' : {'$geoIntersects': {'$geometry': {'type':'Point', 'coordinates':[lon, lat] } } } })

	# Find the nearest parcel
	if results is None:
		results = parcels.find_one({'geometry' : {'$near': {'$geometry': {'type':'Point', 'coordinates':[lon, lat] } } } })
	
	if results is None:
		return None
	else:
		return results['properties']['parcelnum']


# df = pd.read_csv("data/detroit-crime.csv", quotechar='"', low_memory=False)
# for index, row in df.iterrows():
# 	if GetBuilding(row['LAT'], row['LON'], row['ADDRESS']) is None:
# 		print(row)