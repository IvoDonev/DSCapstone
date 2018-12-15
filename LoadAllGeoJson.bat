python LoadGeojson.py -f "data/City of Detroit Boundary.geojson" -d "Capstone" -c "City"
python LoadGeojson.py -f "data/Detroit Neighborhoods.geojson" -d "Capstone" -c "Areas"
python LoadGeojson.py -f "data/Parcel Map.geojson" -d "Capstone" -c "Parcels"
mongo < CreateIndices.js