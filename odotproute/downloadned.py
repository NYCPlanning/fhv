#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import pandas as pd
import urllib.request
import shutil
import zipfile
import os


# Setup project folders
path='C:/Users/Y_Ma2/Desktop/NED/'
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/TRAVELSHEDREVAMP/ned/'


# Download NED data
urllist=pd.read_csv(path+'ned4_20180909_134519.csv')
for i in urllist.index:
    url=urllist.loc[i,'downloadURL']
    title=url.replace('https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/IMG/','')
    title=title.replace('USGS_NED_13_','')
    title=title.replace('_IMG.zip','')
    title=title.replace('.zip','')
    req=urllib.request.urlopen(url)
    file = open(path+title+'.zip', "wb")
    shutil.copyfileobj(req,file)
    file.close()
    zip_ref=zipfile.ZipFile(path+title+'.zip','r')
    zip_ref.extractall(path+title)
    zip_ref.close()
    for j in os.listdir(path+title):
        if j.endswith('.img'):
            shutil.copy(path+title+'/'+j,path+title+'.img')
    os.remove(path+title+'.zip')
    shutil.rmtree(path+title)