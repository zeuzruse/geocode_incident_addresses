# geocode_incident_addresses
Geocoding incident addresses. 

Geocoding incident addresess with US Census Bulk Geocoder, US Census Single Line Address, OSM, and Bing. Will iterate through
directory for any .csv files, pass result from Census Bulk as dataframe to the next three geocoders, and track how long each
geocoder takes, and which addresses were found with which geocoder. 
