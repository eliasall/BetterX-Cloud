
import MySQLdb, re, sys, json, betterX_labs_attributes, ast
from optparse import OptionParser
from cassandra.cluster import Cluster



def openFile(filename):
	f = open(filename, 'r')
	return f

def readWholeFile(file):
	all = f.read()
	return all

def parseJson(tis_string):
	try:
		parsed_json = json.loads(tis_string)
		#return parsed_json
	except ValueError:
		print "error"
		
def is_json(myjson):
	try:
		json_object = json.loads(myjson)
	except ValueError, e:
		return False
	return True
	

##main

### Manually Configure Before Load ###
#################################################################
host = ''
user = ''
password = ''
db = ''
filename = ''
uid = ''
date = ''
tis = {"id": uid, "type": "web", "date": date}
#################################################################

try:
	conn = MySQLdb.connect(host, user, password, db, charset='utf8')
	cursor = conn.cursor()
except Exception as e:
	print str(e)
	sys.exit()
else:
	jiv = 0
	
	if (tis["type"] == 'web'):		
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_labs_attributes.insertWeb(tis["type"], tis_json, cursor, conn, uid)
	

	cursor.close()
	conn.close()
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	