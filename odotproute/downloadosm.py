#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import urllib.request
import shutil

path='/home/mayijun/TRAVELSHED/'
path='J:/TRAVELSHED/travelshedrevamp/'
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2017/TRAVELSHED/travelshedrevamp/'


# Download OpenStreetMap pbf file from geofabrik for walk and transit
# Connecticut
url='https://download.geofabrik.de/north-america/us/connecticut-latest.osm.pbf'
req=urllib.request.urlopen(url)
file = open(path+'otpwt201809/connecticut.osm.pbf', "wb")
shutil.copyfileobj(req,file)
file.close()

# New Jersey
url='https://download.geofabrik.de/north-america/us/new-jersey-latest.osm.pbf'
req=urllib.request.urlopen(url)
file = open(path+'otpwt201809/newjersey.osm.pbf', "wb")
shutil.copyfileobj(req,file)
file.close()

# New York
url='https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf'
req=urllib.request.urlopen(url)
file = open(path+'otpwt201809/newyork.osm.pbf', "wb")
shutil.copyfileobj(req,file)
file.close()

# Pennsylvania
url='https://download.geofabrik.de/north-america/us/pennsylvania-latest.osm.pbf'
req=urllib.request.urlopen(url)
file = open(path+'otpwt201809/pennsylvania.osm.pbf', "wb")
shutil.copyfileobj(req,file)
file.close()


# Download OpenStreetMap pbf file from geofabrik for park and ride
# Connecticut
url='https://download.geofabrik.de/north-america/us/connecticut-latest.osm.pbf'
req=urllib.request.urlopen(url)
file = open(path+'otppr/connecticut.osm.pbf', "wb")
shutil.copyfileobj(req,file)
file.close()

# New Jersey
url='https://download.geofabrik.de/north-america/us/new-jersey-latest.osm.pbf'
req=urllib.request.urlopen(url)
file = open(path+'otppr/newjersey.osm.pbf', "wb")
shutil.copyfileobj(req,file)
file.close()

# New York
url='https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf'
req=urllib.request.urlopen(url)
file = open(path+'otppr/newyork.osm.pbf', "wb")
shutil.copyfileobj(req,file)
file.close()

# Pennsylvania
url='https://download.geofabrik.de/north-america/us/pennsylvania-latest.osm.pbf'
req=urllib.request.urlopen(url)
file = open(path+'otppr/pennsylvania.osm.pbf', "wb")
shutil.copyfileobj(req,file)
file.close()
