########################################################################################
## BetterX Data Importer
## Parses log files and importes them in MySQL & Cassandra (keeping track of imported records)
## Feb 10th 2016
## elias@betterX.org
########################################################################################

import MySQLdb, re, sys, json, betterX_attributes_interactive, ast
from optparse import OptionParser
from cassandra.cluster import Cluster

def parseFilename(filename):
	file_pattern = "^(.*)\/(.*)_(.*)_([0-9]{8}).json$"
	pattern = re.compile(file_pattern)
	result = pattern.match(filename)
	id = str(result.group(2)) if result.group(2) != None else None
	type = str(result.group(3)) if result.group(3) != None else None
	date = str(result.group(4)) if result.group(4) != None else None
	return {"id": id, "type": type, "date": date}

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
CLUSTERS = ['127.0.0.1']
host = ''
user = ''
password = ''
db = ''
filename = ''
uid = ''

try:
	conn = MySQLdb.connect(host, user, password, db, charset='utf8')
	cursor = conn.cursor()
	cluster = Cluster(CLUSTERS)
	session = cluster.connect()
except Exception as e:
	print str(e)
	sys.exit()
else:
	jiv = 0
	tis = parseFilename(filename)
	
	if (tis["type"] == 'tickets'):	
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes_interactive.insertTickets(tis["type"], tis_json, cursor, conn, uid)
	
	if (tis["type"] == 'setup'):	
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes_interactive.insertSetup(tis["type"], tis_json, cursor, conn, uid)
		
	if (tis["type"] == 'sensors'):
		line_count = 0
		tis_file = open(filename, 'r')
		for line in tis_file:
			print line_count
			tis_json = json.loads(str(line))
			print tis_json
			betterX_attributes_interactive.insertSensor(tis["type"], tis_json, cursor, conn, session)
			betterX_attributes_interactive.insertNetwork(tis["type"], tis_json, cursor, conn, uid)
			line_count = line_count + 1
	
	if (tis["type"] == 'network'):	
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes_interactive.insertNetwork(tis["type"], tis_json, cursor, conn, uid)
	
	if (tis["type"] == 'features'):	
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes_interactive.insertFeatures(tis["type"], tis_json, cursor, conn, uid)
	
	if (tis["type"] == 'apps'):		
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes_interactive.insertApps(tis["type"], tis_json, cursor, conn, uid)
	
	if (tis["type"] == 'tabs'):		
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes_interactive.insertTabs(tis["type"], tis_json, cursor, conn, uid)
	
	if (tis["type"] == 'info'):		
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes_interactive.insertInfo(tis["type"], tis_json, cursor, conn, uid)
	
	if (tis["type"] == 'web'):		
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes_interactive.insertWeb(tis["type"], tis_json, cursor, conn, uid)
	

	cursor.close()
	conn.close()
	cluster.shutdown()
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	