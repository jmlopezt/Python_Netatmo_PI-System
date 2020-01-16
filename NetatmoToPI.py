## Juan Manuel López Torralba 2019
## This script downloads data from your stations and 
## X Regions Weather Map by using the Netatmo API.
## Then parse and prepare the data for dumping
## it to a PI System Database by using the Pi Web API.


# -*- coding: utf-8 -*-
import os
import sys
import datetime
import time
import requests
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pprint import pprint
import json
import logging
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
from arcgis.gis import GIS
from arcgis.geocoding import reverse_geocode
from arcgis.geocoding import geocode
import re



# Global variables definition

databaseWebId = "YourAssetsDatabaseWebId" # Asset Database

weatherWebId = "ParentElementWebIdFromAssetsDatabase" # Top(Parent) Element Web ID of PI AF DB

headers = {'Content-Type': 'application/json; charset=utf-8',
           'X-Requested-With':'XMLHttpRequest'}

payload = {'grant_type': 'password',
			'username': "YourUser",
			'password': "YourPassword",
			'client_id':"YourClientID",
			'client_secret': "YourSecret",
			'scope': 'read_station'}


# Functions definition

def GetToken(*args):
	try:
		response = requests.post("https://api.netatmo.com/oauth2/token", data=payload)
		response.raise_for_status()
		access_token=response.json()["access_token"]
		refresh_token=response.json()["refresh_token"]
		scope=response.json()["scope"]

		logging.debug("Your access token is: %s", access_token)
		logging.debug("Your refresh token is: %s", refresh_token)
		logging.debug("Your scopes are: %s", scope)

		return access_token

	except requests.exceptions.HTTPError as error:
		logging.warning('The authentication stage has risen an HTTP error %s %s',error.response.status_code, error.response.text)
		raise

def GetNetatmoData(methodRequest,parametros):
	try:
		url = "https://api.netatmo.com/api/"+methodRequest
		response = requests.post(url, params=parametros)
		response.raise_for_status()
		data = response.json()["body"]
		dataToParsing = response.json()
		return dataToParsing
	except requests.exceptions.HTTPError as error:
		logging.warning("The data downloading stage has risen an HTTP error %s %s", error.response.status_code, error.response.text)
		raise
	except requests.exceptions.ConnectionError as connErr:
		logging.warning("The data downloading stage has risen a Connection Error %s", connErr)
		raise

def dataToFileManagement(target,requestTime,responseTime,data):
	try:
		OwnNetatmoStations_filename = "Own_Netatmo_Stations_year_" + str(datetime.datetime.today().isocalendar()[0]) \
		+ "_week_" + str(datetime.datetime.today().isocalendar()[1]) + ".txt"
		XRegionNetatmoStations_filename = "XRegion_Netatmo_Stations_year_" + str(datetime.datetime.today().isocalendar()[0]) \
		+ "_week_" + str(datetime.datetime.today().isocalendar()[1]) + ".txt"

		if target == 'YourTargetRegions':
			fileStation = open(XRegionNetatmoStations_filename,"a")
		elif target == 'Own':
			fileStation = open(OwnNetatmoStations_filename,"a")

		fileStation.write(str(requestTime)+" ")
		fileStation.write(str(data))
		fileStation.write(" ")
		fileStation.write(str(responseTime)+"\n")
		fileStation.close()

	except Exception as e:
		logging.debug("Rising writing exceptions: %s%", e)
		raise e		

def PublicDataParsing(data):
	_id = []
	pressure=[]
	temperature=[]
	humidity=[]
	HumTempTimestamp = []
	pressureTimestamp = []
	location = []
	altitude = []
	gust_angle = []
	gust_strength = []
	wind_angle = []
	wind_strength = []
	wind_timeutc = []
	rain_24h = []
	rain_60min = []
	rain_live = []
	rain_timeutc = []
	timezone = []

	for devices in data["body"]:
		module_type = []
		for modulo in devices["module_types"]:
			module_type.append(devices["module_types"][modulo])
		if not "NAModule3" in module_type:
			rain_24h.append(None)
			rain_60min.append(None)
			rain_live.append(None)
			rain_timeutc.append(None)
		if not "NAModule2" in module_type:
			gust_angle.append(None)
			gust_strength.append(None)
			wind_angle.append(None)
			wind_strength.append(None)
			wind_timeutc.append(None)
		for elemento in devices["measures"]:
	  		if 'res' in devices["measures"][elemento]:
	  			for timestamp in devices["measures"][elemento]['res']:
	  				length = len(devices["measures"][elemento]['res'][timestamp])
	  				values = devices["measures"][elemento]['res'][timestamp]
	  				if not devices['_id'] in _id:
	  					_id.append(devices['_id'])
	  					altitude.append(devices['place']['altitude'])
	  					location.append(devices['place']['location'])
	  					timezone.append(devices['place']['timezone'])
	  				if len(values) == 1:
	  					pressure.append(values[0])
	  					pressureTimestamp.append(timestamp)
	  				else:
	  					temperature.append(values[0])
	  					humidity.append(values[1])
	  					HumTempTimestamp.append(timestamp)
		  	else:
		  		if len(devices["measures"][elemento]) == 4:
		  			# RAIN
		  			rain_24h.append(devices["measures"][elemento]["rain_24h"])
		  			rain_60min.append(devices["measures"][elemento]["rain_60min"])
		  			rain_live.append(devices["measures"][elemento]["rain_live"])
		  			rain_timeutc.append(devices["measures"][elemento]["rain_timeutc"])
		  		else:
		  			# WIND
		  			gust_angle.append(devices["measures"][elemento]["gust_angle"])
		  			gust_strength.append(devices["measures"][elemento]["gust_strength"])
		  			wind_angle.append(devices["measures"][elemento]["wind_angle"])
		  			wind_strength.append(devices["measures"][elemento]["wind_strength"])
		  			wind_timeutc.append(devices["measures"][elemento]["wind_timeutc"])
	
	if len(gust_angle) < len(_id):
		gust_angle.append(None)
		gust_strength.append(None)
		wind_angle.append(None)
		wind_strength.append(None)
		wind_timeutc.append(None)  


	d = {'Id':_id,'Pressure':pressure,'Pressure_Timestamp':pressureTimestamp,
	    'Temperature':temperature,'Humidity':humidity,'HumidityTemperature_Timestamp':HumTempTimestamp,
	    'Location':location,'Altitude':altitude, 'Rain_24h':rain_24h,'Rain_60min':rain_60min,'Rain_Live':rain_live,
	    'Rain_Timeutc':rain_timeutc,'Gust_Angle':gust_angle,'Gust_Strength':gust_strength,'Wind_Angle':wind_angle,
	    'Wind_Strength':wind_strength,'Wind_Timeutc':wind_timeutc,'Timezone':timezone}

	try:
		df = pd.DataFrame(d)
	except Exception as e:
		df = pd.DataFrame.from_dict(d, orient='index')
		df = df.transpose()
		df['Wind_Timeutc'] = df['Wind_Timeutc'].astype(float)
		df['Rain_Timeutc'] = df['Rain_Timeutc'].astype(float)
		df['Pressure_Timestamp'] = df['Pressure_Timestamp'].astype(float)
		df['HumidityTemperature_Timestamp'] = df['HumidityTemperature_Timestamp'].astype(float)
		return df
	else:
		df['Wind_Timeutc'] = df['Wind_Timeutc'].astype(float)
		df['Rain_Timeutc'] = df['Rain_Timeutc'].astype(float)
		df['Pressure_Timestamp'] = df['Pressure_Timestamp'].astype(float)
		df['HumidityTemperature_Timestamp'] = df['HumidityTemperature_Timestamp'].astype(float)
	
	return df

def PrivateDataParsing(data):

	_id = []
	co2 = []
	last_setup = []
	last_upgrade = []
	wifi_status = []
	firmware = []
	abspressure = []
	internal_co2 = []
	internal_humidity = []
	internal_noise = []
	internal_pressure = []
	internal_temperature = []
	timeutc = []
	altitude = []
	latitude = []
	longitude = []
	reachable = []
	timezone = []
	city = []

	module_id = []
	external_humidity = []
	external_temperature = []
	external_timeutc = []
	module_firmware = []
	module_last_setup = []
	module_battery_vp = []
	module_battery_percent = []
	module_rf_status = []
	module_type = []
	module_reachable = []

	# Internal Modules
	for devices in data["body"]["devices"]:
		try:
			# Configuration Parameters
			_id.append(devices['_id'])
			last_setup.append(devices['last_setup'])
			last_upgrade.append(devices['last_upgrade'])
			wifi_status.append(devices['wifi_status'])
			firmware.append(devices['firmware'])
			reachable.append(devices['reachable'])

			# Dashboard data parameters
			if devices['reachable']:
				abspressure.append(devices['dashboard_data']['AbsolutePressure'])
				internal_co2.append(devices['dashboard_data']['CO2'])
				internal_humidity.append(devices['dashboard_data']['Humidity'])
				internal_noise.append(devices['dashboard_data']['Noise'])
				internal_pressure.append(devices['dashboard_data']['Pressure'])
				internal_temperature.append(devices['dashboard_data']['Temperature'])
				timeutc.append(devices['dashboard_data']['time_utc'])
			else:
				abspressure.append(None)
				internal_co2.append(None)
				internal_humidity.append(None)
				internal_noise.append(None)
				internal_pressure.append(None)
				internal_temperature.append(None)
				timeutc.append(None)

			# Location parameters
			latitude.append(devices['place']['location'][1])
			longitude.append(devices['place']['location'][0])
			altitude.append(devices['place']['altitude'])
			timezone.append(devices['place']['timezone'])
			city.append(devices['place']['city'])
		except Exception as e:
			print("Error in sattion ",devices['station_name'])

	# External Modules
		for modules in devices["modules"]:
			try:
				#Configuration Parameters     
				module_id.append(modules['_id'])
				module_firmware.append(modules['firmware'])
				module_last_setup.append(modules['last_setup'])
				module_battery_vp.append(modules['battery_vp'])
				module_battery_percent.append(modules['battery_percent'])
				module_rf_status.append(modules['rf_status'])
				module_type.append(modules['type'])
				module_reachable.append(modules['reachable'])

				# Dashboard data parameters
				if modules['reachable']:
					external_humidity.append(modules['dashboard_data']['Humidity'])
					external_temperature.append(modules['dashboard_data']['Temperature'])
					external_timeutc.append(modules['dashboard_data']['time_utc'])
				else:
					external_humidity.append(None)
					external_temperature.append(None)
					external_timeutc.append(None)
			except Exception as e:
				print("Error in module:",modules["module_name"])
				raise e


	d = {'Id':_id,'Absolute_Pressure':abspressure,'Internal_Pressure':internal_pressure,'CO2':internal_co2,
	    'Internal_Temperature':internal_temperature,'Humidity':internal_humidity,'Noise':internal_noise,'Indoor_Timestamp':timeutc,
	    'External_Humidity':external_humidity,'External_Temperature':external_temperature,'External_Timestamp':external_timeutc,
	    'Latitude':latitude,'Longitude':longitude,'Altitude':altitude,'City':city,'Timezone':timezone}

	df = pd.DataFrame(d)
	return df

def getCityFromCoordinates(location):
	try:
		gis = GIS()
		results = reverse_geocode(location)
		countryCode = results["address"]["CountryCode"]
		region = results["address"]["Region"]
		subregion = results["address"]["Subregion"]
		city = results["address"]["City"]

		countryCode = re.sub(r"\'", '', countryCode)
		region = re.sub(r"\'", '', region)
		subregion = re.sub(r"\'", '', subregion)
		city = re.sub(r"\'", '', city)

		# if "Catalunya" in region:
		# 	region = "Cataluña"
		# if "Illes Balears" in region:
		# 	region = "Islas Baleares"
		# if "Comunitat Valenciana" in region:
		# 	region = "Comunidad Valenciana"
		# if "Illes Balears" in subregion:
		# 	subregion = "Islas Baleares"
		# if "Alacant" in subregion:
		# 	subregion = "Alicante"

		return countryCode,region,subregion,city

	except Exception as e:
		pprint(location)
		pass

def buildCreateElementPayloadPI(name,description,templateName):
	try:
		if type(name) is str and type(description) is str and type(templateName) is str:
			payloadPI = {	"Name": name,
  						"Description": description,
  						"TemplateName": templateName}
	except Exception as e:
		raise e
		pprint(e)
	return payloadPI

def getWebIdByName(df,name):
	# It is mostly used for obtaining the webid of one specific template
	try:
		df2 = df.loc[df['Name'] == name]
	except Exception as e:
		raise e
	return df2.iloc[0]["Web Id"]

def PostCreateConfig(headers,elementWebId):
	# It creates or updates data reference in the AF DB for the specified element
	try:
		url ="https://YourBaseUrl/piwebapi/elements/"+str(elementWebId)+"/config"
		response = requests.post(url, auth=HTTPBasicAuth("YourUser","YourPassword"), headers=headers)
		response.raise_for_status()
	except requests.exceptions.HTTPError as error:
	    print(error.response.status_code, error.response.text)
	    if error.response.status_code == 207:
	    	print("Process log of operations. Operations completed with errors.")
	else:
		print("Process log of operations. Operations completed with no errors")

def PostCreateElement(headers,payload,ParentElementWebId):
	# It creates a new element in the AF Parent element from the specified template
	try:
		url ="https://YourBaseUrl/piwebapi/elements/"+ParentElementWebId+"/elements"

		s = requests.Session()
		retries = Retry(total=12, backoff_factor=0.1, status_forcelist=[ 502, 503, 504 ])
		s.mount('https://', HTTPAdapter(max_retries=retries))
		s.auth = HTTPBasicAuth("YourUser","YourPassword")
		s.headers.update(headers)

		response = s.post(url, data=json.dumps(payload))
		response.raise_for_status()
	except requests.exceptions.HTTPError as error:
	    print(error.response.status_code, error.response.text)
	    if error.response.status_code == 409:
	    	logging.warning("Ese elemento ya existe, por lo que no se ha creado de nuevo")
	    	raise error
	    else:
	    	print("Elemento creado con éxito:",response.status_code)
	else:
		print("Elemento creado con éxito:",response.status_code)

def GetElementsTemplates(headers,databaseWebId):
	# It retrieves all the element templates in the specified AF database
	try:
		url ="https://YourBaseUrl/piwebapi/assetdatabases/"+databaseWebId+"/elementtemplates"
		s = requests.Session()
		retries = Retry(total=12, backoff_factor=0.2, status_forcelist=[ 502, 503, 504 ])
		s.mount('https://', HTTPAdapter(max_retries=retries))
		s.auth = HTTPBasicAuth("YourUser","YourPassword")
		s.headers.update(headers)

		response = s.get(url)
		response.raise_for_status()
	except requests.exceptions.HTTPError as error:
	    print(error.response.status_code, error.response.text)
	else:
		data = response.json()
	return data

def GetElementsQuery(headers,databaseWebId):
	# It retrives all the existing elements in the specified database
	try:
		url ="https://YourBaseUrl/piwebapi/elements/search?databaseWebId="+databaseWebId+"&maxCount=150000"+"&templateName=YourTemplate"

		s = requests.Session()
		retries = Retry(total=12, backoff_factor=0.1, status_forcelist=[ 502, 503, 504 ])
		s.mount('https://', HTTPAdapter(max_retries=retries))
		s.auth = HTTPBasicAuth("YourUser","YourPassword")
		s.headers.update(headers)

		response = s.get(url)
		response.raise_for_status()
	except requests.exceptions.HTTPError as error:
	    print(error.response.status_code, error.response.text)
	else:
		data = response.json()
	return data

def GetElementFromItsName(headers,ParentElementWebId,nameFilter):
	# It retrives all the existing elements in the specified database with name = namefilter
	# It returns the webid of the desired element
	try:
		url ="https://YourBaseUrl/piwebapi/elements/"+ParentElementWebId+"/elements?nameFilter="+nameFilter
		response = requests.get(url, auth=HTTPBasicAuth("YourUser","YourPassword"), params=headers)
		response.raise_for_status()
	except requests.exceptions.HTTPError as error:
	    print(error.response.status_code, error.response.text)
	else:
		data = response.json()
		webid = data["Items"][0]["WebId"]
	return webid

def compareNewElementsInDB(netatmoDF,afList):
	# Given a DF with new netatmo data and the list with the existing one, it compares whether a new element has appeared in the map.
	# It returns a DF

	#Initialize vars
	afStations_WebId = []
	afStations_Name = []

	try:
		# Obtain a list with the exisitng stations
		for element in afList["Items"]:
			afStations_Name.append(element["Name"])
			afStations_WebId.append(element["WebId"])
		# Adding a new boolean column with true if it a new station
		netatmoDF['New'] = ~(netatmoDF['Id'].isin(afStations_Name))
	except Exception as e:
		raise e
	return (netatmoDF.loc[netatmoDF['New'] == 1])

def buildJsonAdHoc(time_server,netatmoDF,Private):

	body = []
	body.insert(0,{})
	netatmoDF.fillna(value=pd.np.nan,inplace = True)
	timestamp_externo=[]
	timestamp_interno=[]

	#Public DF   #Falta arreglar
	if Private:
		for row in netatmoDF.itertuples():
			if pd.isna(row.External_Timestamp):
				timestamp_externo = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(time_server))
			else:
				timestamp_externo = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.External_Timestamp))
			if pd.isna(row.Indoor_Timestamp):
				timestamp_interno = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(time_server))
			else:
				timestamp_interno = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.Indoor_Timestamp))
			if (pd.notnull(row.Noise) and pd.notnull(row.Noise_WebId)):
				body.append({"WebId":row.Noise_WebId, "Value":{"Timestamp": timestamp_interno, "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Noise}})

			if (pd.notnull(row.Absolute_Pressure) and pd.notnull(row.Absolute_Pressure_WebId)):
				body.append({"WebId":row.Absolute_Pressure_WebId, "Value":{"Timestamp": timestamp_externo, "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Absolute_Pressure}})

			if (pd.notnull(row.Internal_Pressure) and pd.notnull(row.Pressure_WebId)):
				body.append({"WebId":row.Pressure_WebId, "Value":{"Timestamp": timestamp_interno, "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Internal_Pressure}})

			if (pd.notnull(row.CO2) and pd.notnull(row.CO2_WebId)):
				body.append({"WebId":row.CO2_WebId, "Value":{"Timestamp": timestamp_interno, "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.CO2}})

			if (pd.notnull(row.Internal_Temperature) and pd.notnull(row.Internal_Temperature_WebId)):
				body.append({"WebId":row.Internal_Temperature_WebId, "Value":{"Timestamp": timestamp_interno, "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Internal_Temperature}})
		
			if (pd.notnull(row.External_Temperature) and pd.notnull(row.External_Temperature_WebId)):
				body.append({"WebId":row.External_Temperature_WebId, "Value":{"Timestamp": timestamp_externo, "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.External_Temperature}})
			
			if (pd.notnull(row.Humidity) and pd.notnull(row.Internal_Humidity_WebId)):
				body.append({"WebId":row.Internal_Humidity_WebId, "Value":{"Timestamp": timestamp_interno, "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Humidity}})
		
			if (pd.notnull(row.External_Humidity) and pd.notnull(row.External_Humidity_WebId)):
				body.append({"WebId":row.External_Humidity_WebId, "Value":{"Timestamp": timestamp_externo, "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.External_Humidity}})

			if (pd.notnull(row.Altitude) and pd.notnull(row.Altitude_WebId)):
				body.append({"WebId":row.Altitude_WebId, "Value":{"Timestamp": timestamp_externo, "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Altitude}})

			if (pd.notnull(row.Latitude) and pd.notnull(row.Latitude_WebId)):
				body.append({"WebId":row.Latitude_WebId, "Value":{"Timestamp": timestamp_externo, "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Latitude}})

			if (pd.notnull(row.Longitude) and pd.notnull(row.Longitude_WebId)):
				body.append({"WebId":row.Longitude_WebId, "Value":{"Timestamp": timestamp_externo, "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Longitude}})

	else:
		for row in netatmoDF.itertuples():
			if (pd.notnull(row.Pressure) and pd.notnull(row.Pressure_WebId)):
				body.append({"WebId":row.Pressure_WebId, "Value":{"Timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.Pressure_Timestamp)), "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Pressure}})
			
			if (pd.notnull(row.Temperature) and pd.notnull(row.External_Temperature_WebId)):
				body.append({"WebId":row.External_Temperature_WebId, "Value":{"Timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.HumidityTemperature_Timestamp)), "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Temperature}})
			
			if (pd.notnull(row.Humidity) and pd.notnull(row.External_Humidity_WebId)):
				body.append({"WebId":row.External_Humidity_WebId, "Value":{"Timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.HumidityTemperature_Timestamp)), "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Humidity}})
			
			if (pd.notnull(row.Rain_24h) and pd.notnull(row.Rain_24h_WebId)):
				body.append({"WebId":row.Rain_24h_WebId, "Value":{"Timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.Rain_Timeutc)), "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Rain_24h}})

			if (pd.notnull(row.Rain_Live) and pd.notnull(row.Rain_Live_WebId)):
				body.append({"WebId":row.Rain_Live_WebId, "Value":{"Timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.Rain_Timeutc)), "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Rain_Live}})

			if (pd.notnull(row.Rain_60min) and pd.notnull(row.Rain_60min_WebId)):
				body.append({"WebId":row.Rain_60min_WebId, "Value":{"Timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.Rain_Timeutc)), "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Rain_60min}})

			if (pd.notnull(row.Gust_Angle) and pd.notnull(row.Gust_Angle_WebId)):
				body.append({"WebId":row.Gust_Angle_WebId, "Value":{"Timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.Wind_Timeutc)), "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Gust_Angle}})

			if (pd.notnull(row.Gust_Strength) and pd.notnull(row.Gust_Strength_WebId)):
				body.append({"WebId":row.Gust_Strength_WebId, "Value":{"Timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.Wind_Timeutc)), "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Gust_Strength}})

			if (pd.notnull(row.Wind_Angle) and pd.notnull(row.Wind_Angle_WebId)):
				body.append({"WebId":row.Wind_Angle_WebId, "Value":{"Timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.Wind_Timeutc)), "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Wind_Angle}})

			if (pd.notnull(row.Wind_Strength) and pd.notnull(row.Wind_Strength_WebId)):
				body.append({"WebId":row.Wind_Strength_WebId, "Value":{"Timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.Wind_Timeutc)), "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Wind_Strength}})

			if (pd.notnull(row.Altitude) and pd.notnull(row.Altitude_WebId)):
				body.append({"WebId":row.Altitude_WebId, "Value":{"Timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.HumidityTemperature_Timestamp)), "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Altitude}})

			if (pd.notnull(row.Location[0]) and pd.notnull(row.Latitude_WebId)):
				body.append({"WebId":row.Latitude_WebId, "Value":{"Timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.HumidityTemperature_Timestamp)), "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Location[0]}})

			if (pd.notnull(row.Location[1]) and pd.notnull(row.Longitude_WebId)):
				body.append({"WebId":row.Longitude_WebId, "Value":{"Timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(row.HumidityTemperature_Timestamp)), "UnitsAbbreviation": "", "Good": "true", "Questionable": "false", "Value": row.Location[1]}})


	body.pop(0)

	return body

def PostUpdateValueAdHoc(headers,payload):
	# It Updates single value for the specified streams
	try:
		url ="https://YourBaseUrl/piwebapi/streamsets/value"

		s = requests.Session()
		retries = Retry(total=5, backoff_factor=0.2, status_forcelist=[ 502, 503, 504 ])
		s.mount('https://', HTTPAdapter(max_retries=retries))
		s.auth = HTTPBasicAuth("YourUser","YourPassword")
		s.headers.update(headers)

		response = s.post(url, data=json.dumps(payload))
		response.raise_for_status()
	except requests.exceptions.HTTPError as error:
	    print(error.response.status_code, error.response.text)
	    raise error
	    if error.response.status_code == 409:
	    	print("Unsupported operation on the given AF object.")
	    	raise error
	else:
		print("Elemento creado con éxito:",response.status_code)

def addStationWebIDtoDataFrame(netatmoDF,afList):
	# Given a DF with new netatmo data and the list with the existing ones, it appends the webiD of the elements
	# It returns a DF

	stations_WebId = []
	try:
		for row in netatmoDF.itertuples():
			size = len(stations_WebId)
			while len(stations_WebId) < len(netatmoDF.index):
				for afelement in afList["Items"]:
					if row.Id in (afelement['Name']):
						stations_WebId.append(afelement['WebId'])

	except Exception as e:
		raise e
	else:
		try:
			netatmoDF['WebId'] = stations_WebId
		except ValueError:
			stations_WebId.drop()
			netatmoDF['WebId'] = stations_WebId


	return netatmoDF

def getAttribute(headers,elementWebId):
	# It retrieves all the attributes from the specified element
	# GET elements/{webId}/attributes

	try:
		url ="https://YourBaseUrl/piwebapi/elements/"+elementWebId+"/attributes"
		response = requests.get(url, auth=HTTPBasicAuth("YourUser","YourPassword"), params=headers)
		response.raise_for_status()
	except requests.exceptions.HTTPError as error:
	    print(error.response.status_code, error.response.text)
	else:
		data = response.json()
	return data

def addAttributeWebIdToDF(headers,netatmoDF,Private):
	# It is used to append the atributes's webid for each main station to the DF

	latitude_webid = []
	longitude_webid = []
	altitude_webid = []
	timezone_webid = []
	pressure_webid = []
	external_humidity_webid = []
	external_temperature_webid = []

	if Private:
		abspressure_webid = []
		co2_webid = []
		internal_temperature_webid = []
		internal_humidity_webid = []
		noise_webid = []
		city_webid = []

		for row in netatmoDF.itertuples():
			if pd.notnull(row.WebId):
				try:
					atributos = getAttribute(headers,row.WebId) # El resultado es un diccionario
				except Exception as e:
					raise e
				else:
					for attribute in atributos["Items"]:
						if "Internal Temperature" in attribute['Name']:
							internal_temperature_webid.append(attribute['WebId'])
						if "External Temperature" in attribute['Name']:
							external_temperature_webid.append(attribute['WebId'])
						if "CO2" in attribute['Name']:
							co2_webid.append(attribute['WebId'])
						if "Internal Humidity" in attribute['Name']:
							internal_humidity_webid.append(attribute['WebId'])
						if "External Humidity" in attribute['Name']:
							external_humidity_webid.append(attribute['WebId'])
						if "Noise" in attribute['Name']:
							noise_webid.append(attribute['WebId'])
						if "AbsolutePressure" in attribute['Name']:
							abspressure_webid.append(attribute['WebId'])
						if "Altitude" in attribute['Name']:
							altitude_webid.append(attribute['WebId'])
						if "Longitude" in attribute['Name']:
							longitude_webid.append(attribute['WebId'])
						if "Latitude" in attribute['Name']:
							latitude_webid.append(attribute['WebId'])
						if "City" in attribute['Name']:
							city_webid.append(attribute['WebId'])
						if attribute['Name'] == "Pressure":
							pressure_webid.append(attribute['WebId'])
			else:
				internal_temperature_webid.append(None)
				external_temperature_webid.append(None)
				co2_webid.append(None)
				internal_humidity_webid.append(None)
				external_humidity_webid.append(None)
				noise_webid.append(None)
				abspressure_webid.append(None)
				pressure_webid.append(None)
				altitude_webid.append(None)
				longitude_webid.append(None)
				latitude_webid.append(None)
				city_webid.append(None)

		try:
			netatmoDF['Internal_Temperature_WebId'] = internal_temperature_webid
			netatmoDF['External_Temperature_WebId'] = external_temperature_webid
			netatmoDF['CO2_WebId'] = co2_webid
			netatmoDF['Internal_Humidity_WebId'] = internal_humidity_webid
			netatmoDF['External_Humidity_WebId'] = external_humidity_webid
			netatmoDF['Noise_WebId'] = noise_webid
			netatmoDF['Absolute_Pressure_WebId'] = abspressure_webid
			netatmoDF['Pressure_WebId'] = pressure_webid
			netatmoDF['Altitude_WebId'] = altitude_webid
			netatmoDF['Longitude_WebId'] = longitude_webid
			netatmoDF['Latitude_WebId'] = latitude_webid
			netatmoDF['City_WebId'] = city_webid

		except Exception as e:
			raise e
	else:
		rain_24h_webid = []
		rain_60min_webid = []
		rain_live_webid = []
		gust_angle_webid = []
		gust_strength_webid = []
		wind_angle_webid = []
		wind_strength_webid = []
		city_webid = []

		for row in netatmoDF.itertuples():
			if pd.notnull(row.WebId):
				try:
					atributos = getAttribute(headers,row.WebId) # dictionary
				except Exception as e:
					raise e
				else:
					print(pd.notnull(row.WebId))
					print(atributos)
					for attribute in atributos["Items"]:
						if "External Temperature" in attribute['Name']:
							external_temperature_webid.append(attribute['WebId'])
						if "External Humidity" in attribute['Name']:
							external_humidity_webid.append(attribute['WebId'])
						if "Rain_24h" in attribute['Name']:
							rain_24h_webid.append(attribute['WebId'])
						if "Rain_60min" in attribute['Name']:
							rain_60min_webid.append(attribute['WebId'])
						if "Rain Live" in attribute['Name']:
							rain_live_webid.append(attribute['WebId'])
						if "Gust Angle" in attribute['Name']:
							gust_angle_webid.append(attribute['WebId'])
						if "Gust Strength" in attribute['Name']:
							gust_strength_webid.append(attribute['WebId'])
						if "Wind Angle" in attribute['Name']:
							wind_angle_webid.append(attribute['WebId'])
						if "Wind Strength" in attribute['Name']:
							wind_strength_webid.append(attribute['WebId'])
						if "Altitude" in attribute['Name']:
							altitude_webid.append(attribute['WebId'])
						if "Longitude" in attribute['Name']:
							longitude_webid.append(attribute['WebId'])
						if "Latitude" in attribute['Name']:
							latitude_webid.append(attribute['WebId'])
						if "City" in attribute['Name']:
							city_webid.append(attribute['WebId'])
						if attribute['Name'] == "Pressure":
							pressure_webid.append(attribute['WebId'])
			else:
				external_temperature_webid.append(None)
				external_humidity_webid.append(None)
				rain_24h_webid.append(None)
				rain_60min_webid.append(None)
				rain_live_webid.append(None)
				gust_angle_webid.append(None)
				gust_strength_webid.append(None)
				wind_angle_webid.append(None)
				wind_strength_webid.append(None)
				pressure_webid.append(None)
				altitude_webid.append(None)
				longitude_webid.append(None)
				latitude_webid.append(None)
				city_webid.append(None)

		try:
			netatmoDF['External_Temperature_WebId'] = external_temperature_webid
			netatmoDF['External_Humidity_WebId'] = external_humidity_webid
			netatmoDF['Rain_24h_WebId'] = rain_24h_webid
			netatmoDF['Rain_60min_WebId'] = rain_60min_webid
			netatmoDF['Rain_Live_WebId'] = rain_live_webid
			netatmoDF['Gust_Angle_WebId'] = gust_angle_webid
			netatmoDF['Gust_Strength_WebId'] = gust_strength_webid
			netatmoDF['Wind_Angle_WebId'] = wind_angle_webid
			netatmoDF['Wind_Strength_WebId'] = wind_strength_webid
			netatmoDF['Pressure_WebId'] = pressure_webid
			netatmoDF['Altitude_WebId'] = altitude_webid
			netatmoDF['Longitude_WebId'] = longitude_webid
			netatmoDF['Latitude_WebId'] = latitude_webid
			netatmoDF['City_WebId'] = city_webid

		except Exception as e:
			raise e

	return netatmoDF

def buildAFStructure(location):
	# It creates the Parent Region, Subregion and city elements in the AF DB and then retrieves its WebsIDs
	# It returns de webid of the parent element (city) where the stations must be located
	try:
		[countryCode,region,subregion,city] = getCityFromCoordinates(location)

	except Exception as error:
		pprint("Error in the Geoparsing module")

	else:
		if not countryCode == "TUN":
			try:
				rCountryCode = PostCreateElement(headers,buildCreateElementPayloadPI(countryCode,"country code",""),weatherWebId)	# Country Code
			except requests.exceptions.HTTPError as error:
				try:
					rCountryCodeWebId = GetElementFromItsName(headers,weatherWebId,countryCode)
					rRegion = PostCreateElement(headers,buildCreateElementPayloadPI(region,"region",""),rCountryCodeWebId)	# Region
				except Exception as e:
					try:
						rRegionWebId = GetElementFromItsName(headers,rCountryCodeWebId,region)
						rSubregion = PostCreateElement(headers,buildCreateElementPayloadPI(subregion,"subregion",""),rRegionWebId)	# subRegion
					except Exception as e:
						try:
							rSubregionWebId = GetElementFromItsName(headers,rRegionWebId,subregion)
							rCity = PostCreateElement(headers,buildCreateElementPayloadPI(city,"city",""),rSubregionWebId)	# city
						except Exception as e:
							rCityWebId = GetElementFromItsName(headers,rSubregionWebId,city)
						else:
							rCityWebId = rCity['Location'].split('elements/')[1]
					else:
						rSubregionWebId = GetElementFromItsName(headers,rRegionWebId,subregion)
						try:
							rCity = PostCreateElement(headers,buildCreateElementPayloadPI(city,"city",""),rSubregionWebId)	# city
						except Exception as e:
							rCityWebId = GetElementFromItsName(headers,rSubregionWebId,city)
						else:
							rCityWebId = rCity['Location'].split('elements/')[1]
				else:
					try:
						rRegionWebId = rRegion['Location'].split('elements/')[1]
						rSubregion = PostCreateElement(headers,buildCreateElementPayloadPI(subregion,"subregion",""),rRegionWebId)
					except Exception as e:
						rSubregionWebId = GetElementFromItsName(headers,rRegionWebId,subregion)
						try:
							rCity = PostCreateElement(headers,buildCreateElementPayloadPI(city,"city",""),rSubregionWebId)
						except Exception as e:
							rCityWebId = GetElementFromItsName(headers,rSubregionWebId,city)
						else:
							rCityWebId = rCity['Location'].split('elements/')[1]
					else:
						rSubregionWebId = rSubregion['Location'].split('elements/')[1]
						try:
							rCity = PostCreateElement(headers,buildCreateElementPayloadPI(city,"city",""),rSubregionWebId)	# city
						except Exception as e:
							rCityWebId = GetElementFromItsName(headers,rSubregionWebId,city)
						else:
							rCityWebId = rCity['Location'].split('elements/')[1]
			else:
				rCountryCodeWebId = rCountryCode['Location'].split('elements/')[1]
				try:
					rRegion = PostCreateElement(headers,buildCreateElementPayloadPI(region,"region",""),rCountryCodeWebId)	# Region
				except Exception as e:
					rRegionWebId = GetElementFromItsName(headers,rRegionWebId,region)
					try:
						rSubregion = PostCreateElement(headers,buildCreateElementPayloadPI(subregion,"subregion",""),rRegionWebId)	# subRegion
					except Exception as e:
						rSubregionWebId = GetElementFromItsName(headers,rRegionWebId,subregion)
						try:
							rCity = PostCreateElement(headers,buildCreateElementPayloadPI(city,"city",""),rSubregionWebId)	# city
						except Exception as e:
							rCityWebId = GetElementFromItsName(headers,rSubregionWebId,city)
						else:
							rCityWebId = rCity['Location'].split('elements/')[1]
					else:
						rSubregionWebId = rSubregion['Location'].split('elements/')[1]
						try:
							rCity = PostCreateElement(headers,buildCreateElementPayloadPI(city,"city",""),rSubregionWebId)	# subRegion
						except Exception as e:
							rCityWebId = GetElementFromItsName(headers,rSubregionWebId,city)
						else:
							rCityWebId = rCity['Location'].split('elements/')[1]
				else:
					try:
						rRegionWebId = rRegion['Location'].split('elements/')[1]
						rSubregion = PostCreateElement(headers,buildCreateElementPayloadPI(subregion,"subregion",""),rRegionWebId)	# subRegion
					except Exception as e:
						try:
							rSubregionWebId = GetElementFromItsName(headers,rRegionWebId,subregion)
							rCity = PostCreateElement(headers,buildCreateElementPayloadPI(city,"city",""),rSubregionWebId)	# city
						except Exception as e:
							rCityWebId = GetElementFromItsName(headers,rSubregionWebId,city)
						else:
							rCityWebId = rCity['Location'].split('elements/')[1]
					else:
						try:
							rSubregionWebId = rSubregion['Location'].split('elements/')[1]
							rCity = PostCreateElement(headers,buildCreateElementPayloadPI(city,"city",""),rSubregionWebId)	# city
						except Exception as e:
							rCityWebId = GetElementFromItsName(headers,rSubregionWebId,city)
						else:
							rCityWebId = rCity['Location'].split('elements/')[1]

	return rCityWebId


def main():
	logging.basicConfig(filename='netatmo_PiWebApi_Logging.log',level=logging.DEBUG,format='%(asctime)s %(message)s')
	#Consultar en PI los templates de la BD y elegir el óptimo para cada caso

	try:
		stationTemplates = GetElementsTemplates(headers,databaseWebId)
		pprint(stationTemplates)
	except Exception as e:
		print("An exception has been raised in the GetElementsTemplates function")
		pprint(e)
	else:
		if "Netatmo Weather Station" in stationTemplates["Items"][0]["Name"]:
			elementTemplate = stationTemplates["Items"][0]["Name"]
		else:
			pprint("The template does not exist")
	
	

	while True:

		currentMinute = datetime.datetime.now().minute

		if (currentMinute == 0 or \
		currentMinute == 15 or \
		currentMinute == 30 or \
		currentMinute == 45):

			#consultar en Pi los elementos existentes en la BD
			try:
				afElementsQuery = GetElementsQuery(headers,databaseWebId)
			except Exception as e:
				print("An exception has been raised in the GetElementsQuery function")

			try:
				access_token = GetToken(payload)
				pprint(access_token)

			except requests.exceptions.HTTPError as error:
				logging.warning('Cannot get token')
			else:
				token = {
				'access_token': access_token
				}

			try:
				requestTime = datetime.datetime.now()
				logging.debug('%s',requestTime)
				logging.debug("The download of the Own Netatmo modules has STARTED")
				data = GetNetatmoData('getstationsdata',token)
			except requests.exceptions.HTTPError as error:
				logging.warning('Cannot get the data from our stations')

			else:
				responseTime = datetime.datetime.now()
				logging.debug('%s',responseTime)
				logging.debug("The download of the Own Netatmo modules has FINISHED")
				try:
					dataToFileManagement('Own',requestTime,responseTime,data)
					#Parsear 
					privateDF = PrivateDataParsing(data)
					private_time_server = data['time_server']
					pprint(privateDF)
					try:
						#privateDF = addStationWebIDtoDataFrame(privateDF,afElementsQuery)
						# Comparar si existen nuevas estaciones que no esten en nuestra BD
						newStationsFromPrivateDF = compareNewElementsInDB(privateDF,afElementsQuery)
						#pprint(newStationsFromPrivateDF)
					except Exception as e:
						logging.debug("There is a problem in the comparison of new elements in the Private modules")
						raise e
					else:
						# Añadir al AF
						# 1. Creamos el formato de payload y los elementos en PI
						contador = 0
						for row in newStationsFromPrivateDF.itertuples():
							payloadPI = buildCreateElementPayloadPI(row.Id,"Netatmo Weather Station",elementTemplate)
							city = newStationsFromPrivateDF.at[row.Index,'City']
							# build AF structure for private modules
							try:
								gis = GIS()
								geocodeSTR = city+",Spain"
								results = geocode(geocodeSTR)
							except Exception as e:
								pprint("The geoparsing module has failed for the Private locations")
								logging.debug("The geoparsing module throws failures for the Private locations")
							else:
								try:
									location = results[0]['location']
									rCityWebId = buildAFStructure(location)
								except Exception as e:
									pprint("There is a problem with the AF structure")
									logging.debug("There is a problem with the AF structure construction for the Private Modules")
								else:
									try:
										PostCreateElement(headers,payloadPI,rCityWebId)
									except Exception as e:
										pprint("The element already exists")
										logging.debug("The element already exists / Private DF")
									else:
										contador += 1
									
						try:
							afElementsQuery = GetElementsQuery(headers,databaseWebId)
						except Exception as e:
							print("An exception has been raised in the GetElementsQuery function")
						else:
							try:
								privateDF = addStationWebIDtoDataFrame(privateDF,afElementsQuery)
							except Exception as e:
								logging.debug("There are problem by adiing the stations WebId to the DF/Private")

						for row in privateDF.itertuples():
							try:
								# Update config
								PostCreateConfig(headers,row.WebId)
							except Exception as e:
								logging.debug("There is a problem with the updateConfig call/Private")
								raise e
							else:
								logging.debug("Private Pi Points were successfully created")

						try:
							# Get the webid of the whole attributes for each station and then append it to the dataframe
							privateDF = addAttributeWebIdToDF(headers,privateDF,1)
						except Exception as e:
							logging.debug("There is a problem with the addition of the attributes webids to the Private DF")
						else:
							try:
								# Crear Json con datos 
								payloadjson = buildJsonAdHoc(private_time_server,privateDF,1)
								pprint(payloadjson)
							except Exception as e:
								logging.error(e)
								logging.debug("There is a problem with the Private data Json creation")
								raise e
							else:
								try:
									# Send Data to PI
									PostUpdateValueAdHoc(headers,payloadjson)
								except Exception as e:
									logging.error(e)
									logging.debug("There is a problem with the Private data dumping")
									raise e
								else:
									logging.debug("Private Data have been successfully dumped into PI")

				except Exception as e:
					logging.warning('Cannot write data in file')
					raise e
		

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
			# Coordinates from a Z rectangular region (Change it for your coordinates)
			params.append({
				'access_token': access_token,
				'lat_ne': 11.11,
				'lon_ne': -11.11,
				'lat_sw': 22.22,
				'lon_sw': -22.22
				})
			# Coordinates from a XY rectangular region (Change it for your coordinates)
			params.append({
				'access_token': access_token,
				'lat_ne': 11.11,
				'lon_ne': -11.11,
				'lat_sw': 22.22,
				'lon_sw': -22.22
				})

			regionCount = 1
			currentMinute = datetime.datetime.now().minute

			for regiones in params:
			#currentMinute = datetime.datetime.now().minute
				if (currentMinute == 0 or \
					currentMinute == 15 or \
					currentMinute == 30 or \
					currentMinute == 45):
						try:
							requestTime = datetime.datetime.now()
							logging.debug('%s',requestTime)
							logging.debug("Weather map region %d download stage has STARTED", regionCount)
							dataWMap = GetNetatmoData('getpublicdata',regiones)

						except requests.exceptions.HTTPError as error:
							logging.warning("Cannot get data from region %d", regionCount)
							time.sleep(120) # waits 2 minutes

							try:
								requestTimeLOG = datetime.datetime.now()
								logging.debug('%s',requestTimeLOG)
								logging.debug("Weather map region %d download stage has re-STARTED", regionCount)
								dataWMap = GetNetatmoData('getpublicdata',regiones)

							except requests.exceptions.HTTPError as error2:
								logging.warning("Cannot get data from region %d", regionCount)
								time.sleep(120)

								try:
									requestTimeLOG = datetime.datetime.now()
									logging.debug('%s',requestTimeLOG)
									logging.debug("Weather map region %d download stage has re-STARTED", regionCount)
									dataWMap = GetNetatmoData('getpublicdata',regiones)

								except requests.exceptions.ConnectionError as connErr:
									logging.warning("Cannot get data from region %d due to a connection error", regionCount)
									time.sleep(300) #duerme 5 minutos
								except Exception as e:
									logging.error("The following exception from region %d is throwing problems: %s", regionCount, e)
									time.sleep(900) #duerme 15 minutos
								else:
									responseTime = datetime.datetime.now()
									logging.debug('%s',responseTime)
									logging.debug("Weather map region %d download stage has FINISHED", regionCount)
						
							else:
								responseTime = datetime.datetime.now()
								logging.debug('%s',responseTime)
								logging.debug("Weather map region %d download stage has FINISHED", regionCount)

						except ValueError:
							logging.citical("I/O operation failed")
						except Exception as e:
							logging.error("Some error had not been handled and the region %d has not been downloaded: %s", regionCount,e)

						else:
							time.sleep(10)
							responseTime = datetime.datetime.now()
							logging.debug('%s',responseTime)
							logging.debug("Weather map region %d download stage has FINISHED", regionCount)

						finally: 
							if regionCount < 4:
								regionCount += 1
							else:
								regionCount = 1
				
						dataToFileManagement('Spain',requestTime,responseTime,dataWMap)
						#Parsing
						try:
							publicDF = PublicDataParsing(dataWMap)
							public_time_server = dataWMap['time_server']
						except Exception as e:
							logging.debug("The Public Data parsing throws errors")
							raise e
						else:
							# Not interested in Tunis Data now
							publicDF = publicDF[publicDF.Timezone != 'Africa/Tunis']
							pprint(publicDF)
							newStationsFromPublicDF = compareNewElementsInDB(publicDF,afElementsQuery)
							# Add to PI AF
							contador = 0
							for row in newStationsFromPublicDF.itertuples():
								payloadPI = buildCreateElementPayloadPI(row.Id,"Netatmo Weather Station",elementTemplate)
								# reverse geocoding
								location = newStationsFromPublicDF.at[row.Index,'Location']
								try:
									rCityWebId = buildAFStructure(location)
								except Exception as e:
									logging.debug("There is a problem with the AF structure construction for the Public Modules")
								else:
									try:
										PostCreateElement(headers,payloadPI,rCityWebId)
									except Exception as e:
										pprint("The element already exists / Public DF")
										logging.debug("The element already exists / Public DF")
									else:
										contador += 1

							try:
								afElementsQuery = GetElementsQuery(headers,databaseWebId)
							except Exception as e:
								print("An exception has been raised in the GetElementsQuery function")
							else:
								try:
									publicDF = addStationWebIDtoDataFrame(publicDF,afElementsQuery)
								except Exception as e:
									logging.debug("There are problem by adiing the stations WebId to the DF/Public")
									try:
										afElementsQuery = GetElementsQuery(headers,databaseWebId)
										publicDF = addStationWebIDtoDataFrame(publicDF,afElementsQuery)
									except Exception as e:
										logging.debug("There are problem by adiing the stations WebId to the DF/Public")
										time.sleep(25)
										try:
											afElementsQuery = GetElementsQuery(headers,databaseWebId)
											time.sleep(5)
											publicDF = addStationWebIDtoDataFrame(publicDF,afElementsQuery)
										except Exception as e:
											pprint(e)
											raise e

							for row in publicDF.itertuples():
								try:
									# Update config
									if pd.notnull(row.webId):
										PostCreateConfig(headers,row.WebId)
								except Exception as e:
									pprint("There is a problem with the updateConfig call/Public")
									# raise e
								else:
									logging.debug("Private Pi Points were successfully created")
									pprint("Pi Point creados con éxito/Public")

							try:
								# Get the webid of the whole attributes for each station and then append it to the dataframe
								publicDF = addAttributeWebIdToDF(headers,publicDF,0)
							except Exception as e:
								logging.debug("There is a problem by adding the attributes webids to the Public DF")
								try:
									afElementsQuery = GetElementsQuery(headers,databaseWebId)
									time.sleep(5)
									publicDF = addStationWebIDtoDataFrame(publicDF,afElementsQuery)
								except Exception as e:
									pprint(e)
									for row in publicDF.itertuples():
										try:
											# Update config
											PostCreateConfig(headers,row.WebId)
										except Exception as e:
											logging.warning("There is a problem with the updateConfig call/Public")
											raise e
										else:
											logging.debug("Public Pi Points were successfully created")
											try:
												afElementsQuery = GetElementsQuery(headers,databaseWebId)
												time.sleep(5)
												publicDF = addStationWebIDtoDataFrame(publicDF,afElementsQuery)
											except Exception as e:
												logging.error(e)
												raise e
									try:
										publicDF = addAttributeWebIdToDF(headers,publicDF,0)
									except Exception as e:
										raise e
									else:
										try:
											# Crear Json con datos 
											payloadjson = buildJsonAdHoc(public_time_server,publicDF,0)
										except Exception as e:
											logging.error(e)
											logging.error("There is a problem with the Public data Json creation")
											raise e
										else:
											try:
												# Enviar datos a PI
												PostUpdateValueAdHoc(headers,payloadjson)
											except Exception as e:
												pprint(e)
												logging.error(e)
												logging.error("There is a problem with the Public data dumping")
												raise e
											else:
												logging.debug("Public Data have been successfully dumped into PI")
									
									try:
										# Crear Json con datos 
										payloadjson = buildJsonAdHoc(public_time_server,publicDF,0)
									except Exception as e:
										#pprint(payloadjson)
										pprint(e)
										logging.error(e)
										logging.debug("There is a problem with the Public data Json creation")
										raise e
									else:
										try:
											#pprint(payloadjson)
											# Enviar datos a PI
											PostUpdateValueAdHoc(headers,payloadjson)
										except Exception as e:
											pprint(e)
											logging.error(e)
											logging.debug("There is a problem with the Public data dumping")
											raise e
										else:
											logging.debug("Public Data have been successfully dumped into PI")
								else:
									publicDF = addAttributeWebIdToDF(headers,publicDF,0)
									try:
										# Crear Json con datos 
										payloadjson = buildJsonAdHoc(public_time_server,publicDF,0)
										#pprint(payloadjson)
									except Exception as e:
										#pprint(payloadjson)
										pprint(e)
										logging.error(e)
										logging.debug("There is a problem with the Public data Json creation")
										raise e
									else:
										try:
											#pprint(payloadjson)
											# Enviar datos a PI
											PostUpdateValueAdHoc(headers,payloadjson)
										except Exception as e:
											pprint(e)
											logging.error(e)
											logging.debug("There is a problem with the Public data dumping")
											raise e
										else:
											logging.debug("Public Data have been successfully dumped into PI")
							else:
								try:
									# Crear Json con datos 
									payloadjson = buildJsonAdHoc(public_time_server,publicDF,0)
									pprint(payloadjson)
								except Exception as e:
									logging.error(e)
									logging.debug("There is a problem with the Public data Json creation")
									raise e
								else:
									try:
										# Enviar datos a PI
										PostUpdateValueAdHoc(headers,payloadjson)
									except Exception as e:
										pprint(e)
										logging.error(e)
										logging.debug("There is a problem with the Public data dumping")
										raise e
									else:
										logging.debug("Public Data have been successfully dumped into PI")

		time.sleep(30)

if __name__ == '__main__':
    main()

