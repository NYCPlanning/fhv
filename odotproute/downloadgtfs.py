#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import urllib.request
import shutil

# Setup project folders
#path='/home/mayijun/TRAVELSHED/otpwt201809/'
#path='J:/TRAVELSHED/travelshedrevamp/'
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/otpwt201809/'



# Download GTFS data

# New York
# MTA NYCT Subway
url='http://web.mta.info/developers/data/nyct/subway/google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'mtanyctsubway.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# MTA NYCT Bus Bronx
url='http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip'
req=urllib.request.urlopen(url)
file = open(path+'mtanyctbusbronx.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# MTA NYCT Bus Brooklyn
url='http://web.mta.info/developers/data/nyct/bus/google_transit_brooklyn.zip'
req=urllib.request.urlopen(url)
file = open(path+'mtanyctbusbrooklyn.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# MTA NYCT Bus Manhattan
url='http://web.mta.info/developers/data/nyct/bus/google_transit_manhattan.zip'
req=urllib.request.urlopen(url)
file = open(path+'mtanyctbusmanhattan.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# MTA NYCT Bus Queens
url='http://web.mta.info/developers/data/nyct/bus/google_transit_queens.zip'
req=urllib.request.urlopen(url)
file = open(path+'mtanyctbusqueens.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# MTA NYCT Bus Staten Island
url='http://web.mta.info/developers/data/nyct/bus/google_transit_staten_island.zip'
req=urllib.request.urlopen(url)
file = open(path+'mtanyctbusstatenisland.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# MTA Bus Company
url='http://web.mta.info/developers/data/busco/google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'mtabusco.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# MTA long Island Railroad (LIRR)
url='http://web.mta.info/developers/data/lirr/google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'mtalirr.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# MTA Metro-North Railroad (MNR)
url='http://web.mta.info/developers/data/mnr/google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'mtamnr.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Port Authority Trans-Hudson (PATH)
url='http://data.trilliumtransit.com/gtfs/path-nj-us/path-nj-us.zip'
req=urllib.request.urlopen(url)
file = open(path+'path.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# JFK AirTrain (manually download the 29 July 2015 version and change the calendar)
print('https://transitfeeds.com/p/jfk-airtrain/433')

# NYC DOT Staten Island Ferry
url='http://www.nyc.gov/html/dot/downloads/misc/siferry-gtfs.zip'
req=urllib.request.urlopen(url)
file = open(path+'nycdotsiferry.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# NYC Ferry
url='http://nycferry.connexionz.net/rtt/public/utility/gtfs.aspx?google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'nycferry.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# NY Waterway
url='https://s3.amazonaws.com/data.bytemark.co/nywaterway/nywaterway.zip'
req=urllib.request.urlopen(url)
file = open(path+'nywaterway.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Seastreak
url='http://seastreak.com/api/transit/google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'seastreak.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# NYC Downtown Alliance
url='http://mjcaction.com/MJC_GTFS_Public/downtown_nyc_google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'downtownalliance.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Nassau Inter-County Express Bus (NICE; manually download the file for the most up to date version)
print('https://www.nicebus.com/Passenger-Information/App-showcase')

# Suffolk County Transit (http://www.suffolkcountyny.gov/Departments/PublicWorks/Transportation/GeneralTransitFeedSpecificationGTFSDetails.aspx)
url='http://www.suffolkcountyny.gov/portals/0/publicworks/zip/google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'suffolk.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Westchester County Bee-Line System (NYS511: https://511ny.org/developers/resources)
url='https://s3.amazonaws.com/datatools-nysdot/public/Westchester_County_Bee-Line_System.zip'
req=urllib.request.urlopen(url)
file = open(path+'beeline.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# TappanZee Express (Rockland TZX; NYS511: https://511ny.org/developers/resources)
url='https://s3.amazonaws.com/datatools-nysdot/public/TappanZee_Express.zip'
req=urllib.request.urlopen(url)
file = open(path+'tzx.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Ulster County Area Transit (UCAT; NYS511: https://511ny.org/developers/resources)
url='https://s3.amazonaws.com/datatools-nysdot/public/UCAT_Ulster_County_Area_Transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'ucat.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Capital District Transportation Authority (CDTA)
url='http://www.cdta.org/schedules/google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'cdta.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Rochester-Genesee Regional Transportation Authority (RGRTA; NYS511: https://511ny.org/developers/resources)
url='http://scheduledata.rgrta.com/google_transit_merged.zip'
req=urllib.request.urlopen(url)
file = open(path+'rgrta.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Niagara Frontier Transportation Authority (NFTA)
url='http://www.nfta.com/metro/__googletransit/google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'nfta.zip', "wb")
shutil.copyfileobj(req,file)
file.close()


# New Jersey
# New Jersey Transit (manually download the file; need to log in)
print('https://www.njtransit.com/mt/mt_servlet.srv?hdnPageAction=MTDevResourceDownloadTo&Category=rail')
print('https://www.njtransit.com/mt/mt_servlet.srv?hdnPageAction=MTDevResourceDownloadTo&Category=bus')


# Connecticut
# Connecticut Transit
url='https://www.cttransit.com/sites/default/files/gtfs/googlect_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'cttransit.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Shore Line East
url='http://www.shorelineeast.com/google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'sle.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# 9 Town Transit
url='http://data.trilliumtransit.com/gtfs/ninetown-connecticut-us/ninetown-connecticut-us.zip'
req=urllib.request.urlopen(url)
file = open(path+'9town.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Norwalk Transit District
url='https://www.norwalktransit.com/s/GTFS_Data.zip'
req=urllib.request.urlopen(url)
file = open(path+'norwalk.zip', "wb")
shutil.copyfileobj(req,file)
file.close()


# Pennsylvania
# Port Authority Transit Corporation (PATCO)
url='http://www.ridepatco.org/developers/PortAuthorityTransitCorporation.zip'
req=urllib.request.urlopen(url)
file = open(path+'patco.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Southeastern Pennsylvania Transportation Authority (SEPTA; manually download the file and separate bus and rail)
print('http://www3.septa.org/') # Media=>Developer=>GTFS=>github

# Monroe County Transit Authority (Pocono Pony)
url='https://www.gomcta.com/google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'pocono.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Rabbit Transit
url='http://www.rabbittransit.org/infopoint/gtfs/google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'rabbit.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Centre County Transit Authority (CATA)
url='https://catabus.com/data/google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'cata.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Port Authority of Allegheny County (PAAC)
url='http://www.portauthority.org/GeneralTransitFeed/google_transit.zip'
req=urllib.request.urlopen(url)
file = open(path+'paac.zip', "wb")
shutil.copyfileobj(req,file)
file.close()

# Erie Metropolitan Transit Authority (EMTA; manually download the file for the most up to date version and change calendar)
print('http://www.ride-the-e.com/wp-content/uploads/2017/09/EMTA_GTFS.zip')

# Lehigh and Northampton Transportation Authority (LANTA)
url='https://github.com/LANTA-Transportation-Authority/GTFS-data/raw/master/lanta_gtfs_feed.zip'
req=urllib.request.urlopen(url)
file = open(path+'lanta.zip', "wb")
shutil.copyfileobj(req,file)
file.close()


# Rhode Island
# Rhode Island Public Transit Authority (RIPTA) (manually download the file for the most up to date version)
print('https://www.ripta.com/mobile-applications')


# Delaware
# Delaware Transit Corporation (DART)
url='https://dartfirststate.com/information/routes/gtfs_data/dartfirststate_de_us.zip'
req=urllib.request.urlopen(url)
file = open(path+'dart.zip', "wb")
shutil.copyfileobj(req,file)
file.close()


## Problematic feeds
## USA
## Amtrak (transitland FOIA)
#url='http://github.com/transitland/gtfs-archives-not-hosted-elsewhere/raw/master/amtrak.zip'
#req=urllib.request.urlopen(url)
#file = open(path+'amtrak.zip', "wb")
#shutil.copyfileobj(req,file)
#file.close()

## Centro (NYS511: https://511ny.org/developers/resources)
#url='https://www.centro.org/CentroGTFS/CentroGTFS.zip'
#req=urllib.request.urlopen(url)
#file = open(path+'centro.zip', "wb")
#shutil.copyfileobj(req,file)
#file.close()