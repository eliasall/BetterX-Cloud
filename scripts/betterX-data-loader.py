########################################################################################
## BetterX Data Loader
## Loads files from AWS S3 and processes them
## Jan 14th 2016
## elias@betterX.org
########################################################################################

import boto3, re, os, MySQLdb, sys
## AWS S3 
## Libraries from https://github.com/boto/boto3
## Setup Files in ~/.aws

## CONFIG
S3_BUCKET = ''
S3_BUCKET_ARCHIVE = ''
DOWNLOAD_PATH = ''
UNZIP_PATH = ''
FILE_ERR_PATH = ''
DECRYPT_ALGO = ''
DECRYPT_KEY = ''
DECRYPT_INIT_VECTOR = ''
DB_HOST = ''
DB_USER = ''
DB_PASSWORD = ''
DB_NAME = ''
IMPORTER = 'betterX-db-importer.py'

def checkMySQLConnectionCursor(conn, cursor):
	if (conn.open == 0):
		conn.reconnect(attempts=100, delay=10)
		cursor = conn.cursor()
	
	return conn, cursor

def getMySQLConnectionCursor(conn, cursor, host, user, password, name):
	if (conn == None):
		conn = MySQLdb.connect(host, user, password, name, charset='utf8')
		cursor = conn.cursor()
	
	if (conn.open == 0):
		conn.reconnect(attempts=100, delay=10)
		cursor = conn.cursor()
	
	return conn, cursor

def parseFilename(filename):
	pattern = re.compile("^(.*)_([0-9]{8}).zip.enc$")
	result = pattern.match(filename)
	return [result.group(1), result.group(2)]

def deleteFilename(filename):
	os.remove(filename)

def dbinsert(tblName,fields,fieldTypes,cursor,values,conn):
	sql_command = "insert into " + tblName + " (" + fields + ") values (" + fieldTypes + ")"
	cursor.execute(sql_command, values)
	conn.commit()
	
## MAIN
s3 = boto3.resource('s3')
bucket = s3.Bucket(S3_BUCKET)
exists = True
try:
    s3.meta.client.head_bucket(Bucket=S3_BUCKET)
except botocore.exceptions.ClientError as e:
    # If a client error is thrown, then check that it was a 404 error.
    # If it was a 404 error, then the bucket does not exist.
    error_code = int(e.response['Error']['Code'])
    if error_code == 404:
        exists = False

try:
	conn = None
	cursor = None
	conn, cursor = getMySQLConnectionCursor(conn, cursor, DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)
except Exception as e:
	print str(e)
	sys.exit()	

zip_file_counter = 0
for keyN in bucket.objects.all():
	zip_file_counter = zip_file_counter + 1


inner_zip_file_counter = 0
for key in bucket.objects.all():
	inner_zip_file_counter = inner_zip_file_counter + 1
	print (str(key.key) + "   " + str(key.last_modified))
	s3.Object(S3_BUCKET, str(key.key)).download_file(DOWNLOAD_PATH +str(key.key))
	file_parse_results = parseFilename(str(key.key))
	tis_userid = file_parse_results[0]
	tis_date = file_parse_results[1]
	original_filename = str(key.key)
	exported_filename = str(key.key).replace('.enc', '')
	os.system('openssl enc -d ' + DECRYPT_ALGO + '  -in ' + DOWNLOAD_PATH + original_filename + ' -out ' + DOWNLOAD_PATH +  exported_filename + ' -K ' + DECRYPT_KEY + ' -iv ' + DECRYPT_INIT_VECTOR) # decrypt
	os.system('unzip -qq ' + DOWNLOAD_PATH + exported_filename + ' -d ' + UNZIP_PATH) # unzip
	inner_file_counter = 0
	for dir_entry in os.listdir(UNZIP_PATH):	# for each file in temp directory
		dir_entry_path = os.path.join(UNZIP_PATH, dir_entry)
		if os.path.isfile(dir_entry_path):
			inner_file_counter = inner_file_counter + 1
			filename = dir_entry_path
			print "   " + filename
			cmd = 'python ' + IMPORTER + ' -m ' + DB_HOST + ' -u ' + DB_USER + ' -p ' + "'" + DB_PASSWORD + "'" + ' -d ' + DB_NAME + ' -f ' + filename + ' -i ' +  tis_userid + ' -a ' + tis_date
			result = os.system(cmd)
			tis_status = 'OK'
			if (result != 0):
				tis_status = result
			conn, cursor = checkMySQLConnectionCursor(conn, cursor)
			dbinsert('file_log', 'file_zip, file_name, file_uid, file_time, status, zip_file, zip_file_no, zip_file_total', '%s,%s,%s,%s,%s,%s,%s,%s', cursor, [original_filename,filename,tis_userid,tis_date,tis_status,str(inner_file_counter), str(inner_zip_file_counter), str(zip_file_counter)], conn)
			if (tis_status == 'OK'):
				deleteFilename(filename)
			else:
				os.system('cp ' + filename + ' ' + FILE_ERR_PATH)
				deleteFilename(filename)
	deleteFilename(DOWNLOAD_PATH + original_filename) 
	deleteFilename(DOWNLOAD_PATH + exported_filename)
	s3.Object(S3_BUCKET_ARCHIVE,str(key.key)).copy_from(CopySource=S3_BUCKET+'/'+str(key.key))
	s3.Object(S3_BUCKET,str(key.key)).delete()

cursor.close()
conn.close()