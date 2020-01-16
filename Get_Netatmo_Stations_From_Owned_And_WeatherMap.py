## Juan Manuel López Torralba 2019
## This script downloads data from your
## Netatmo Stations and the Netatmo Weather 
## Map by using the Netatmo API

## The code can be improve it by aplying best practices with Requests and Retry.



# -*- coding: utf-8 -*-
import os
import sys
import datetime
import time
import requests
from pprint import pprint
import json
import logging

#RequestPeriod = input("Please enter the request period in seconds: ")

logging.basicConfig(filename='YourFilename.log',level=logging.DEBUG,format='%(asctime)s %(message)s')

#### 1. Obtain the netatmo token associated with your account

payload = {'grant_type': 'password',
					 'username': "Your Username",
					 'password': "Your Password",
					 'client_id':"Your Client Id",
					 'client_secret': "Your Client Secret",
					 'scope': 'read_station'}



while True:
	fileStation = open("YourSavedStationsHere.txt","a")
	try:
		response = requests.post("https://api.netatmo.com/oauth2/token", data=payload)
		response.raise_for_status()
		access_token=response.json()["access_token"]
		refresh_token=response.json()["refresh_token"]
		scope=response.json()["scope"]
		logging.debug("Your access token is: %s", access_token)
		logging.debug("Your refresh token is: %s", refresh_token)
		logging.debug("Your scopes are: %s", scope)
	except requests.exceptions.HTTPError as error:
		logging.warning('The authentication stage has risen an HTTP error %s %s',error.response.status_code, error.response.text)


	#### 2. Download netatmo data from your owned stations

	params = {
		'access_token': access_token
	}

	try:
		logging.debug("Your Own Netatmo stations download stage has STARTED")
		requestTime = datetime.datetime.now()
		response = requests.post("https://api.netatmo.com/api/getstationsdata", params=params)
		response.raise_for_status()
		data = response.json()["body"]
		responseTime = datetime.datetime.now()
		logging.debug("Your Own Netatmo stations download stage has FINISHED")

	except requests.exceptions.HTTPError as error:
		print(error.response.status_code, error.response.text)
		logging.warning('Your Own Netatmo stations download stage has risen an HTTP error %s %s',error.response.status_code, error.response.text)


	fileStation.write(str(requestTime)+" ")
	fileStation.write(str(data))
	fileStation.write(" ")
	fileStation.write(str(responseTime)+"\n")
	fileStation.close()

		#### 3. Download netatmo data from weather map
		#### 3.1 Defining areas

	params = []
	# Coordinates from a X rectangular region (Change it for your coordinates)
	params.insert(0,{
		'access_token': access_token,
		'lat_ne': 11.11,
		'lon_ne': -11.11,
		'lat_sw': 22.22,
		'lon_sw': -22.22
		})
	# Coordinates from a Y rectangular region (Change it for your coordinates)
	params.append({
		'access_token': access_token,
		'lat_ne': 11.11,
		'lon_ne': -11.11,
		'lat_sw': 22.22,
		'lon_sw': -22.22
		})


	regionCount = 1

	for regiones in params:
		try:
			fileWeatherMap = open("WeatherMapStationsHere.txt","a")
			logging.debug("Weather map region %d download stage has STARTED", regionCount)
			requestTime = datetime.datetime.now()
			responseWMap = requests.post("https://api.netatmo.com/api/getpublicdata", params=regiones)
			responseWMap.raise_for_status()
			dataWMap = responseWMap.json()["body"]
			responseTime = datetime.datetime.now()
			logging.debug("Weather map region %d download stage has FINISHED", regionCount)
			time.sleep(60) # Specify whether or not you want sleep the thread.

			if regionCount < 4:
				regionCount += 1
			else:
				regionCount = 1

		except requests.exceptions.HTTPError as error:
			logging.warning("The region %d download has risen an HTTP error %s %s", regionCount, error.response.status_code, error.response.text)
			time.sleep(300) # Sometimes the API returns Error for access limitations
			try:
				responseWMap = requests.post("https://api.netatmo.com/api/getpublicdata", params=regiones)
				responseWMap.raise_for_status()
				logging.debug("Weather map region %d download stage has FINISHED", regionCount)

				if regionCount < 4:
					regionCount += 1
				else:
					regionCount = 1

			except requests.exceptions.HTTPError as error2:
				print(error2.response.status_code, error2.response.text)
				logging.warning("The region %d download has risen an HTTP error %s %s", regionCount, error2.response.status_code, error2.response.text)
				time.sleep(300) # Improve it in the future by using the Retry package
				try:
					responseWMap = requests.post("https://api.netatmo.com/api/getpublicdata", params=regiones)
					logging.debug("Weather map region %d download stage has FINISHED", regionCount)

					if regionCount < 4:
						regionCount += 1
					else:
						regionCount = 1

				except requests.exceptions.ConnectionError as connErr:
					time.sleep(900) 
					logging.warning("The region %d download has risen a Connection Error %s", regionCount, connErr)
		except ValueError:
			logging.citical("I/O operation failed")
		finally:
			logging.error("Some error had not been handled and the region %d has not been downloaded", regionCount)
				
			
		fileWeatherMap.write(str(requestTime)+" ")
		fileWeatherMap.write(str(dataWMap))
		fileWeatherMap.write(" ")
		fileWeatherMap.write(str(responseTime)+"\n")
		fileWeatherMap.close()

	time.sleep(900)