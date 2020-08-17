#! /usr/bin/python3
# Travelshed Mapping
# OpenTripPlanner Guide: http://docs.opentripplanner.org/en/latest/
# OpenTripPlanner API Doc: http://dev.opentripplanner.org/apidoc/1.0.0/index.html

import urllib.request
import shutil

path='/home/mayijun/TRAVELSHED/'
path='J:/TRAVELSHED/travelshedrevamp/'
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2017/TRAVELSHED/travelshedrevamp/'


# Download OpenTripPlanner jar file for walk and transit
# Double check the update before downloading: https://repo1.maven.org/maven2/org/opentripplanner/otp/
url='https://repo1.maven.org/maven2/org/opentripplanner/otp/1.3.0/otp-1.3.0-shaded.jar'
req=urllib.request.urlopen(url)
file = open(path+'otpwt201809/otp-1.3.0-shaded.jar', "wb")
shutil.copyfileobj(req,file)
file.close()


# Download OpenTripPlanner jar file for park and ride
# Double check the update before downloading: https://repo1.maven.org/maven2/org/opentripplanner/otp/
url='https://repo1.maven.org/maven2/org/opentripplanner/otp/1.3.0/otp-1.3.0-shaded.jar'
req=urllib.request.urlopen(url)
file = open(path+'otppr/otp-1.3.0-shaded.jar', "wb")
shutil.copyfileobj(req,file)
file.close()