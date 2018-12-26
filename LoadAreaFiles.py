from pymongo import MongoClient, GEOSPHERE, ASCENDING
from pymongo.errors import (PyMongoError, BulkWriteError)
import shapely
from shapely.geometry import mapping, shape, Point
import json
import QueryBuildings

def ExceptionHandler(bwe):
	nInserted = bwe.details["nInserted"]
	errMsg = bwe.details["writeErrors"]
	print("Errors encountered inserting features")
	print("Number of Features successully inserted:", nInserted)
	print("The following errors were found:")
	for item in errMsg:
		print("Index of feature:" , item["index"])
		print("Error code:", item["code"])
		print("Message (truncated due to data length):", item["errmsg"][0:120], "...")


client = MongoClient()
db = client["Capstone"]

# Start with the city boundary 
print("Inserting City Boundary")

inputfile = "data/City of Detroit Boundary.geojson"
geojson = {}

with open(inputfile,'r') as f:
	geojson = json.loads(f.read())

city = db["City"]
city.drop() # clear out first

city.create_index([("geometry", GEOSPHERE)])
bulk = city.initialize_unordered_bulk_op()

for feature in geojson['features']:
	bulk.insert(feature)

try:
	result = bulk.execute()
	print("Number of Features successully inserted:", result["nInserted"])
except BulkWriteError as bwe:
	ExceptionHandler(bwe)

print("Inserting City Areas")

# Start with the city neighborhoods
inputfile = "data/Detroit Neighborhoods.geojson"
areas = db["Areas"]
areas.drop()  # clear out first

with open(inputfile,'r') as f:
	geojson = json.loads(f.read())

areas.create_index([("properties.target_fid", ASCENDING)])
areas.create_index([("geometry", GEOSPHERE)])
bulk = areas.initialize_unordered_bulk_op()

areaMap= {}

for feature in geojson['features']:
	sh = shape(feature["geometry"])
	areaMap[feature["properties"]["target_fid"]] = sh
	feature["lat"] = sh.centroid.y
	feature["lon"] = sh.centroid.x
	bulk.insert(feature)
try:
	result = bulk.execute()
	print("Number of Features successully inserted:", result["nInserted"])
except BulkWriteError as bwe:
	ExceptionHandler(bwe)

print("Inserting City Parcel Info")

# Start with the city neighborhoods
inputfile = "data/Parcel Map.geojson"
parcels = db["Parcels"] # clear out first
parcels.drop() 

with open(inputfile,'r') as f:
	geojson = json.loads(f.read())

parcels.create_index([("properties.parcelnum", ASCENDING)])
parcels.create_index([("properties.address", ASCENDING)])
parcels.create_index([("geometry", GEOSPHERE)])
bulk = parcels.initialize_unordered_bulk_op()

counter = 0
total = len(geojson['features'])

for feature in geojson['features']:
	sh = shape(feature["geometry"])

	# show how far we've gotten through the data
	counter += 1
	if counter%100 == 0:
		print(counter, " out of ", total)

	# find which area our parcel is in
	nhoodID = QueryBuildings.GetArea(sh.centroid.y, sh.centroid.x)
	if nhoodID is None:
		nhoodID = 0

	# add extra information for each feature
	feature["nhood_id"] = nhoodID
	feature["lat"] = sh.centroid.y
	feature["lon"] = sh.centroid.x
	bulk.insert(feature)
try:
	result = bulk.execute()
	print("Number of Features successully inserted:", result["nInserted"])
except BulkWriteError as bwe:
	ExceptionHandler(bwe)
