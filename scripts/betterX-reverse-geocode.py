#!/usr/bin/python
# coding: utf8

import os, urllib, hmac, binascii, base64, hashlib, urllib2, json, pprint, simplejson, sys, time
import MySQLdb
from optparse import OptionParser
import codecs

host = ''
user = ''
password = ''
db = ''
dbtable = ''
google_private_key = ''
query = 'select latitude, longitude from ' + dbtable + ' where address is null'

def sendRequest(latlng, private_key, slow, position):
	GOOGLE_API_DOMAIN = 'https://maps.googleapis.com'
	GOOGLE_API_URL = '/maps/api/geocode/'
	RESULT_TYPE = 'json'
	SENSOR_VALUE = 'false'
	encodedParams = urllib.urlencode({'latlng': unicode(latlng), 'key': private_key,  'sensor': SENSOR_VALUE});	
	signed_url = GOOGLE_API_DOMAIN + GOOGLE_API_URL + RESULT_TYPE + '?' + encodedParams
	time.sleep(slow)
	try:	
		data = urllib2.urlopen(signed_url)
		j = json.load(data)
		return j
	except:
		time.sleep(slow)
		sendRequest(latlng, private_key, slow, position)

try:
	conn = MySQLdb.connect (host, user, password, db, charset='utf8')
except:
	print ">> Unable to connect...exit"
	sys.exit()
else:
	cursor = conn.cursor()
	cursor.execute (query)
	rows = cursor.fetchall()
	numrows = int(cursor.rowcount)
	cnt_ok = 0
	cnt_zero = 0
	cnt_quota = 0
	cnt_denied = 0
	cnt_invalid = 0
	cnt_error = 0
	cnt_dbupdates = 0
	slow = 0
	for i in range(numrows):
		latlng = str(rows[i][0]) + "," + str(rows[i][1])
		jsn = sendRequest(latlng, google_private_key, slow, i)
		if jsn is not None:	
			results = jsn["results"]
			status = jsn["status"]
			print status
			try:
				test = results[0]['formatted_address']
			except:
				test = None
			if (test != None):
				try:
					print str(rows[i][0]) + "," + str(rows[i][1]) + "," + results[0]['formatted_address']
					sql_command = "update " + dbtable + " set `address` = %s  where `Latitude` = %s and `Longitude` = %s"
					cursor.execute(sql_command,(results[0]['formatted_address'],rows[i][0], rows[i][1]))
					conn.commit()
				except ValueError:
					print ValueError
			else:
				print str(rows[i][0]) + "," + str(rows[i][1])			
		if status == 'OVER_QUERY_LIMIT':
			slow = slow + 0.2
			print slow
		
	cursor.close()