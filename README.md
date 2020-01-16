# Python_Netatmo_PI-System
Python scripting files for automated data downloading from Netatmo Weather API and its interaction with the PI System.

These scritps allows you to download, parse, handle and dump the most updated data from Netatmo stations to your PI System (Osisoft).

Get public stations records for specific dates and ranges.
Get the last Private and Public Stations data from Netatmo.
Parsing Example for obtaining specific data such as Temperature or Wind Gauge.
NetatmoToPi: A summary for download, parsing and data handling of Netatmo data. It also creates the whole set of Pi Points needed 
in the Pi Archive, then the AF structure of the assets in your database, a specific JSON for uploading data according to the PI Web
API requirements and finally data dumping.

## Getting Started

The script files are independent of each other.

### Prerequisites

Python 3

Libraries: 

Requests, pprint, logging, Pandas, Numpy, Arcgis

## Deployment

It is neccesary to update the Username, password and several IDs with your own case.

## Author

Juan Manuel López Torralba

## License

This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike CC BY-NC-SA License

This license lets others remix, tweak, and build upon your work non-commercially, as long as they credit you and license their new creations under the identical terms.

To view a copy of the license, visit https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode

