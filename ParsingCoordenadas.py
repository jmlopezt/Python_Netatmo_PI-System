## Juan Manuel López Torralba 2019
## This script parse the coordinates of 
## Netatmo Stations from the retrieved 
## Json File

# -*- coding: utf-8 -*-
import os
import sys
import datetime
import time
import requests
from pprint import pprint
import json
import logging
import re
import numpy as np


## Parsing from your owned stations ##
fileOwnStation = open("YourFileName.txt","a")
cont = 0
cont2 = 0
myList = []
myList2 = []
asdf = []

with open ('FileWithJsonResponse.txt') as f:  # Json from api/getstationsdata
	content = f.readlines()

	for line in content:
		myList = line.split("u'altitude': ")  # it thows 7 elements per row.
		for z in myList:
			if cont !=0: 
				altitude = z.split(", u'location': [")
				if len(altitude[0]) < 5:
					pprint(altitude[0])
					fileOwnStation.write(altitude[0]+",")
				for l in altitude: 
					location = l.split("], u'country")
					if (len(location[0]) > 4) and (len(location[0]) < 100):
						print(location[0])
						fileOwnStation.write(location[0]+"\n")
			if cont < len(myList):
				cont += 1
			else:
				cont = 0
fileOwnStation.close()


## Parsing from big region Weather Map stations ##
fileWholeStation = open("YourPublicDataFileName.txt","a")

with open ('FileWithJsonResponseWeatherMap.txt') as f:  # Json from /api/getpublicdata
	content = f.readlines()

	for line in content:
		myList = line.split("u'altitude': ") 
		for z in myList:
			if cont !=0: 
				altitude = z.split(", u'location': [")
				if len(altitude[0]) < 20:
					fileWholeStation.write(altitude[0]+",")
				for l in altitude: 
					location = l.split("]}, u'_id': u'")
					if ((len(location[0]) > 4) and (len(location[0]) < 50) and location[0] != altitude[0]):
						fileWholeStation.write(location[0]+",")
						for idStation in location:
							if cont == len(myList):
								identifier = idStation.split("'") 
							else:
								identifier = idStation.split("'") 
							if ((len(identifier[0]) == 17) and identifier[0] != location[0]):
								fileWholeStation.write(identifier[0]+"\n")
			if cont <= len(myList)+1:
				cont += 1
			else:
				cont = 0

				fileWholeStation.write("\n")
				fileWholeStation.write("\n")
				fileWholeStation.write("\n")

fileWholeStation.close()
