
## Web File
def insertWeb(filetype, json, cursor, conn, uid):
	if (filetype == 'web'):
		web_page_node(json,uid,cursor,conn) 		# [pages] / [pageNode]
		web_entry_node(json, uid, cursor, conn)		# [pages] / [entriesNode]

def web_entry_response(json_entries_node, uid, cursor, conn, parentid):
	tblName = 'lab_web_entries_response'
	featureAttrs = {'status', 'statusText', 'httpVersion', 'cookieNumber', 'redirectURL', 'headersSize', 'bodySize'}
	featureAttrs2 = {'Date', 'Server', 'X-Powered-By', 'Content-Encoding', 'Content-Length', 'Keep-Alive', 'Connection', 'Content-Type'}
	featureAttrs3 = {'size', 'compression', 'mimeType', 'encoding'}
	vals = {}
	values = []
	cntattr = 0
	for tis in featureAttrs:
		vals[cntattr] = tis
		values.append(json_entries_node['response'][tis])
		cntattr = cntattr + 1
	
	vals[cntattr] =  'web_entries_id'
	values.append(parentid)
	cntattr = cntattr + 1
	attrsInJson,typesInJson = toCommaStringDict(vals)
	
	#print type(attrsInJson)
	#print attrsInJson
	
	vals2 = {}
	values2 = []
	cntattr2 = 0
	for tis2 in featureAttrs2:
		vals2,values2 = appendJsonKey(json_entries_node['response']['headers'], tis2, vals2, values2, cntattr2)
		cntattr2 = cntattr2 + 1
	
	renameArrayItem(vals2, 'Date', 'header_Date')
	renameArrayItem(vals2, 'Server', 'header_Server')
	renameArrayItem(vals2, 'X-Powered-By', 'header_XPoweredBy')
	renameArrayItem(vals2, 'Content-Encoding', 'header_ContentEncoding')
	renameArrayItem(vals2, 'Content-Length', 'header_ContentLength')
	renameArrayItem(vals2, 'Keep-Alive', 'header_KeepAlive')
	renameArrayItem(vals2, 'Connection', 'header_Connection')
	renameArrayItem(vals2, 'Content-Type', 'header_ContentType')
	
	attrsInJson2,typesInJson2 = toCommaStringDict(vals2)
	#print type(attrsInJson2)
	#print attrsInJson2
	
	vals3 = {}
	values3 = []
	cntattr3 = 0
	for tis3 in featureAttrs3:
		vals3,values3 = appendJsonKey(json_entries_node['response']['content'], tis3, vals3, values3, cntattr3)
		cntattr3 = cntattr3 + 1
	
	renameArrayItem(vals3, 'size', 'content_size')
	renameArrayItem(vals3, 'compression', 'content_compression')
	renameArrayItem(vals3, 'mimeType', 'content_mimeType')
	renameArrayItem(vals3, 'encoding', 'content_encoding')
	
	attrsInJson3,typesInJson3 = toCommaStringDict(vals3)
	#print type(attrsInJson3)
	#print attrsInJson3	
	
	attrsInJsonCombined = attrsInJson
	typesInJsonCombined = typesInJson
	if ( attrsInJson2 != ''):
		attrsInJsonCombined = attrsInJsonCombined + ',' + attrsInJson2
		typesInJsonCombined = typesInJsonCombined + ',' + typesInJson2
		values.extend(values2)
	if ( attrsInJson3 != ''):
		attrsInJsonCombined = attrsInJsonCombined + ',' + attrsInJson3
		typesInJsonCombined = typesInJsonCombined + ',' + typesInJson3
		values.extend(values3)
		
	dbinsert(tblName,attrsInJsonCombined,typesInJsonCombined,cursor,values,conn)
		
def web_entry_request(json_entries_node, uid, cursor, conn, parentid):
	tblName = 'lab_web_entries_request'
	featureAttrs = {'method', 'url', 'httpVersion', 'cookieNumber', 'headerSize', 'bodySize'}
	featureAttrs2 = {'Host', 'User-Agent', 'Accept', 'Accept-Encoding', 'Connection', 'Content-Length', 'Keep-Alive'}
	vals = {}
	values = []
	cntattr = 0
	for tis in featureAttrs:
		vals[cntattr] = tis
		values.append(json_entries_node['request'][tis])
		cntattr = cntattr + 1
	
	vals[cntattr] =  'web_entries_id'
	values.append(parentid)
	cntattr = cntattr + 1
	attrsInJson,typesInJson = toCommaStringDict(vals)
	
	#print type(attrsInJson)
	#print attrsInJson
	
	vals2 = {}
	values2 = []
	cntattr2 = 0
	for tis2 in featureAttrs2:
		vals2,values2 = appendJsonKey(json_entries_node['request']['headers'], tis2, vals2, values2, cntattr2)
		cntattr2 = cntattr2 + 1
	
	
	renameArrayItem(vals2, 'Host', 'header_Host')
	renameArrayItem(vals2, 'User-Agent', 'header_UserAgent')
	renameArrayItem(vals2, 'Accept', 'header_Accept')
	renameArrayItem(vals2, 'Accept-Encoding', 'header_AcceptEncoding')
	renameArrayItem(vals2, 'Connection', 'header_Connection')
	renameArrayItem(vals2, 'Content-Length', 'header_ContentLength')
	renameArrayItem(vals2, 'Keep-Alive', 'header_KeepAlive')
	
	attrsInJson2,typesInJson2 = toCommaStringDict(vals2)
	#print type(attrsInJson2)
	#print attrsInJson2
	
	attrsInJsonCombined = attrsInJson
	typesInJsonCombined = typesInJson
	if ( attrsInJson2 != ''):
		attrsInJsonCombined = attrsInJson + ',' + attrsInJson2
		typesInJsonCombined = typesInJson + ',' + typesInJson2
		values.extend(values2)
		
	dbinsert(tblName,attrsInJsonCombined,typesInJsonCombined,cursor,values,conn)
	
def web_entry_node(json, uid, cursor, conn):
	tblName = 'lab_web_entries'
	featureAttrs = {'pageid', 'entryStartTime', 'time', 'serverIPAddress', 'connection'}
	featureAttrs2 = {'blocked', 'dns', 'connect', 'send', 'wait', 'receive', 'ssl'}
	featureAttrs3 = {'beforeRequestCacheEntries', 'afterRequestCacheEntries', 'hitCount'}
	for jiv in json['pages']:
		for innerjiv in jiv['entriesNode']:
			
			cntattr = 0
			attrsInJson = ''
			typesInJson = ''
			keytypevals = {}
			values = []
			for tis in featureAttrs:
				keytypevals,values = appendJsonKey(innerjiv, tis, keytypevals, values, cntattr)
				cntattr = cntattr + 1
			attrsInJson,typesInJson = toCommaStringDict(keytypevals)
			
			cntattr2 = 0
			attrsInJson2 = ''
			typesInJson2 = ''
			keytypevals2 = {}
			values2 = []
			for tis2 in featureAttrs2:
				keytypevals2,values2 = appendJsonKey(innerjiv['timings'], tis2, keytypevals2, values2, cntattr2)
				cntattr2 = cntattr2 + 1
			attrsInJson2,typesInJson2 = toCommaStringDict(keytypevals2)
			
			cntattr3 = 0
			attrsInJson3 = ''
			typesInJson3 = ''
			keytypevals3 = {}
			values3 = []
			for tis3 in featureAttrs3:
				keytypevals3,values3 = appendJsonKey(innerjiv['cache'], tis3, keytypevals3, values3, cntattr3)
				cntattr3 = cntattr3 + 1
			attrsInJson3,typesInJson3 = toCommaStringDict(keytypevals3)
			
			##combine
			attrsInJsonCombined = attrsInJson + ',' + attrsInJson2 + ',' + attrsInJson3
			typesInJsonCombined = typesInJson + ',' + typesInJson2 + ',' + typesInJson3
			values.extend(values2)
			values.extend(values3)
			#insert
			dbinsert(tblName,attrsInJsonCombined,typesInJsonCombined,cursor,values,conn)
			
			##entry request
			web_entry_id = getMaxId(tblName,cursor,conn)
			web_entry_request(innerjiv, uid, cursor, conn, web_entry_id)
			web_entry_response(innerjiv, uid, cursor, conn, web_entry_id)
					
def web_page_node(json, uid, cursor, conn):
	tblName = 'lab_web_pages'
	featureAttrs = {'tabid', 'pageStartTime', 'pageid', 'pagetitle', 'pageOnContentLoad', 'pageOnLoad', 'origin'}
	cntattr = 0
	for jiv in json['pages']:
		attrsInJson = ''
		typesInJson = ''
		keytypevals = {}
		values = []
		for tis in featureAttrs:
			keytypevals,values = appendJsonKey(jiv['pageNode'], tis, keytypevals, values, cntattr)
			cntattr = cntattr + 1
		keytypevals[cntattr] =  'uid'
		cntattr = cntattr + 1
		values.append(uid)
		renameArrayItem(keytypevals, 'pageid', 'id')
		attrsInJson,typesInJson = toCommaStringDict(keytypevals)
		dbinsert(tblName,attrsInJson,typesInJson,cursor,values,conn)		



## Helper Functions	

			
def dbinsert(tblName,fields,fieldTypes,cursor,values,conn):
	sql_command = "insert into " + tblName + " (" + fields + ") values (" + fieldTypes + ")"
	#print sql_command
	#print values
	cursor.execute(sql_command, values)
	conn.commit()

def getMaxId(tblName,cursor, conn):
	sql = "select max(id) from " + tblName
	cursor.execute(sql)
	results = cursor.fetchall()
	return str(results[0][0])
	
def isJsonKey(json, tisKey):
	for key,val in json.items():
		if (key == tisKey):
			return True
			break
	
	return False
	
def appendJsonKey(json, key, vals, values, cntattr):
	if (isJsonKey(json,key)):
		vals[cntattr] = str(key)
		values.append(json[key])
	return vals,values

def toCommaStringDict(keytypevals):
	ret = ''
	ret2 = ''
	for key in keytypevals:
		ret = ret + '`' + keytypevals[key] + '`' + ','
		ret2 = ret2 + '%s' + ','
	if (len(ret) > 0):
		ret = ret[:-1]
		ret2 = ret2[:-1]
	return ret,ret2

def renameArrayItem(arr, frm, to):
	for key in arr:
		try:
			if( arr[key] == frm):
				arr[key] = to
		except:
			dummy = 0
	return arr	

def appendJsonKeyConcat(json, key, vals, values, cntattr):
	ret = ''
	if (isJsonKey(json,key)):
		for i in json[key]:
			ret = (ret + ' ' + i).strip()
		vals[cntattr] = str(key)
		values.append(ret)
	return vals,values