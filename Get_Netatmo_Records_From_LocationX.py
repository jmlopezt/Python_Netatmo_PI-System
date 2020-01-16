## Juan Manuel López Torralba 2019
## This script downloads historical data from 
## X City Weather Map by using the Netatmo API



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
import re




# Global variables definition

date_begin = 11111111 # Your start date in epoch.
step_time =  86400 # Set the step time for retrieving data.

headers = {'Content-Type': 'application/json; charset=utf-8',
           'X-Requested-With':'XMLHttpRequest'}

payload = {'grant_type': 'password',
			'username': "Your Username",
			'password': "Your Password",
			'client_id':"Your Client Id",
			'client_secret': "Your Client Secret",
			'scope': 'read_station'}





def GetToken(*args):
	try:
		response = requests.post("https://api.netatmo.com/oauth2/token", data=payload)
		response.raise_for_status()
		access_token=response.json()["access_token"]
		refresh_token=response.json()["refresh_token"]
		scope=response.json()["scope"]
		return access_token

	except requests.exceptions.HTTPError as error:
		logging.warning('The authentication stage has risen an HTTP error %s %s',error.response.status_code, error.response.text)
		raise


def GetNetatmoData(methodRequest,parametros):
	try:
		url = "https://api.netatmo.com/api/"+methodRequest
		pprint(url)
		pprint(parametros)
		response = requests.post(url, params = parametros)
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
	except Exception as e:
		pprint(e)
		raise


def parsingIdNetatmoData(data):

	df = pd.DataFrame({'id': devices["_id"], 'modulos': devices["modules"]} for devices in data["body"])
	return df

def parsingTemperatureData(data, _id, modulo):

	df = pd.DataFrame({'id': _id,'modulo': modulo,'beg_time': devices["beg_time"], 'temperature': devices["value"]} for devices in data["body"])
	return df

def dataToFile(data):
	try:
		filename = "YourXLocationNetatmoRecord" + ".txt"
		fileStation = open(filename,"a")
		fileStation.write(str(data))
		fileStation.write("\n")
		fileStation.close()
	except Exception as e:
		logging.debug("Rising writing exceptions: %s%", e)
		raise e	


def main():

	currentMinute = datetime.datetime.now().minute
	df2 = pd.DataFrame(columns=['id','modulo','beg_time','temperature'])
	df_temperature = pd.DataFrame(columns=['id','modulo','beg_time','temperature'])

	try:
		access_token = GetToken(payload)
	except requests.exceptions.HTTPError as error:
		logging.warning('Cannot get token')
	else:
		token = {
		'access_token': access_token
		}
		regiones = ({
			'access_token': access_token,
			'lat_ne': 11.1111,
			'lon_ne': -8.88,
			'lat_sw': 11.12,
			'lon_sw': -8.88,
			'filter': "true"
		}) # Change the coordinates for real ones.

		try:
			data = GetNetatmoData('getpublicdata',regiones)
		except requests.exceptions.HTTPError as error:
			logging.warning('Cannot get the data from the stations')
		else:
			df = parsingIdNetatmoData(data)	
			### getcommondata
			for row in df.itertuples(index=False):
				_id = row.id
				for modulo in row.modulos:
				 	pprint(modulo)
				 	try:
				 		parametros = ({
						'access_token': access_token,
						'device_id': _id,
						'module_id': modulo,
						#'scale': '1day',
						'scale': '1hour',
						'type': 'Temperature',
						'filter': "true",
						'date_begin':date_begin,
						'optimize': 'true',
						'real_time': 'true'
						})
				 		data = GetNetatmoData('getmeasure',parametros)
				 	except Exception as e:
				 		pass
				 	else:
				 		df2.append(df2, ignore_index = True)
				 		if data is not None:
				 			df2 = parsingTemperatureData(data, _id,modulo)
				df_temperature = pd.concat([df_temperature,df2], ignore_index = True)
			
			try:
				dataToFile(df_temperature)
			except Exception as e:
				print(e)
				pass
			else:
				df_temperature.to_csv('YourDataFrameInCsv.csv')
				df_temperature.to_excel("YourDataFrameInExcel.xlsx")

if __name__ == '__main__':
    main()