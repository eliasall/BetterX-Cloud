########################################################################################
## BetterX Data Importer
## parses log files and importes them in MySQL & Cassandra
## Jan 14th 2016
## elias@betterX.org
########################################################################################

import MySQLdb, re, sys, json, betterX_attributes, ast
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
usage = "usage: %prog -m -u -p -d -f -t -a"
parser = OptionParser(usage=usage)
parser.add_option("-m", dest="host", help="host for mysql database")
parser.add_option("-u", dest="user", help="username for mysql database")
parser.add_option("-p", dest="password", help="password for mysql database")
parser.add_option("-d", dest="db", help="target mysql database name")
parser.add_option("-f", dest="filename", help="log filename")
parser.add_option("-i", dest="uid", help="uid")
parser.add_option("-a", dest="date", help="date")
(options, args) = parser.parse_args()

CLUSTERS = ['127.0.0.1']
host = options.host
user = options.user
password = options.password
db = options.db
filename = options.filename
uid = options.uid
date = options.date

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
	if (uid != tis["id"] or date != tis["date"]):
		sys.exit("*** Error >> Parsed uid/date mismatch")
	
	if (tis["type"] == 'tickets'):	
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes.insertTickets(tis["type"], tis_json, cursor, conn, uid)
	
	if (tis["type"] == 'setup'):	
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes.insertSetup(tis["type"], tis_json, cursor, conn, uid)
		
	if (tis["type"] == 'sensors'):
		line_count = 0
		tis_file = open(filename, 'r')
		for line in tis_file:
			tis_json = json.loads(str(line))
			betterX_attributes.insertSensor(tis["type"], tis_json, cursor, conn, session)
			betterX_attributes.insertNetwork(tis["type"], tis_json, cursor, conn, uid)
			line_count = line_count + 1
	
	if (tis["type"] == 'network'):	
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes.insertNetwork(tis["type"], tis_json, cursor, conn, uid)
	
	if (tis["type"] == 'features'):	
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes.insertFeatures(tis["type"], tis_json, cursor, conn, uid)
	
	if (tis["type"] == 'apps'):		
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes.insertApps(tis["type"], tis_json, cursor, conn, uid)
	
	if (tis["type"] == 'tabs'):		
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes.insertTabs(tis["type"], tis_json, cursor, conn, uid)
	
	if (tis["type"] == 'info'):		
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes.insertInfo(tis["type"], tis_json, cursor, conn, uid)
	
	if (tis["type"] == 'web'):		
		tis_file = open(filename, 'r')
		tis_json = json.loads(tis_file.read())
		betterX_attributes.insertWeb(tis["type"], tis_json, cursor, conn, uid)
	

	cursor.close()
	conn.close()
	cluster.shutdown()
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	