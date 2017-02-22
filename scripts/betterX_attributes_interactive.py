## Tickets File
def insertTickets(filetype, json, cursor, conn, uid):
	if (filetype == 'tickets'):
		featureAttrs = {'tickets', 'timestamp', 'uid'}
		cnt = 0
		tblName = 'tickets'
		cntattr = 0
		keytypevals = {}
		values = []
		for tis in featureAttrs:
			keytypevals,values = appendJsonKey(json, tis, keytypevals, values, cntattr)
			cntattr = cntattr + 1
		renameArrayItem(keytypevals, 'timestamp', 'epoch')
		attrsInJson,typesInJson = toCommaStringDict(keytypevals)
		dbinsert(tblName,attrsInJson,typesInJson,cursor,values,conn)

## Setup File
def insertSetup(filetype, json, cursor, conn, uid):
	if (filetype == 'setup'):
		featureAttrs = {'age', 'city', 'country', 'datatransmit_time', 'education', 'gender', 'phoneusefrequency', 'timezone', 'uid', 'webusefrequency', 'latitude', 'longitude', 'timestamp', 'datatransmit_charging', 'datatransmit_wifi'}
		cnt = 0
		tblName = 'setup'
		cntattr = 0
		keytypevals = {}
		values = []
		for tis in featureAttrs:
			keytypevals,values = appendJsonKey(json, tis, keytypevals, values, cntattr)
			cntattr = cntattr + 1
		attrsInJson,typesInJson = toCommaStringDict(keytypevals)
		dbinsert(tblName,attrsInJson,typesInJson,cursor,values,conn)

## Web File
def insertWeb(filetype, json, cursor, conn, uid):
	if (filetype == 'web'):
		web_page_node(json,uid,cursor,conn) 		# [pages] / [pageNode]
		web_entry_node(json, uid, cursor, conn)		# [pages] / [entriesNode]

def web_entry_response(json_entries_node, uid, cursor, conn, parentid):
	tblName = 'web_entries_response'
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
	tblName = 'web_entries_request'
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
	tblName = 'web_entries'
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
	tblName = 'web_pages'
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

## Tab File
def insertTabs(filetype, json, cursor, conn, uid):
	if (filetype == 'tabs'):
		featureAttrs = {'timestamp', 'tabid', 'tabstatus'}
		cnt = 0
		for jiv in json['tabs']:
			tblName = 'web_tabs'
			cntattr = 0
			keytypevals = {}
			values = []
			for tis in featureAttrs:
				keytypevals,values = appendJsonKey(jiv, tis, keytypevals, values, cntattr)
				cntattr = cntattr + 1
			
			keytypevals[cntattr] =  'uid'
			cntattr = cntattr + 1
			values.append(uid)
			attrsInJson,typesInJson = toCommaStringDict(keytypevals)
			dbinsert(tblName,attrsInJson,typesInJson,cursor,values,conn)

## Info File
def insertInfo(filetype, json, cursor, conn, uid):
	if (filetype == 'info'):
		featureAttrs = {'timestamp', 'version', 'browser'}
		tblName = 'web_info'
		cntattr = 0
		keytypevals = {}
		values = []
		for tis in featureAttrs:
			keytypevals,values = appendJsonKey(json, tis, keytypevals, values, cntattr)
			cntattr = cntattr + 1
			
		keytypevals[cntattr] =  'uid'
		cntattr = cntattr + 1
		values.append(uid)
		attrsInJson,typesInJson = toCommaStringDict(keytypevals)
		dbinsert(tblName,attrsInJson,typesInJson,cursor,values,conn)

## App File		
def insertApps(filetype, json, cursor, conn, uid):
	if (filetype == 'apps'):
		featureAttrs = {'uid', 'timestamp', 'app'}
		cnt = 0
		for jiv in json:
			tblName = 'apps'
			cntattr = 0
			keytypevals = {}
			values = []
			for tis in featureAttrs:
				keytypevals,values = appendJsonKey(jiv, tis, keytypevals, values, cntattr)
				cntattr = cntattr + 1
			
			attrsInJson,typesInJson = toCommaStringDict(keytypevals)
			dbinsert(tblName,attrsInJson,typesInJson,cursor,values,conn)

## Feature File
def insertFeatures(filetype, json, cursor, conn, uid):
	if (filetype == 'features'):
		featureAttrs = {'manufacturer', 'model', 'timestamp', 'uid', 'version'}
		cnt = 0
		for jiv in json:
			tblName = 'features'
			cntattr = 0
			keytypevals = {}
			values = []
			for tis in featureAttrs:
				keytypevals,values = appendJsonKey(jiv, tis, keytypevals, values, cntattr)
				cntattr = cntattr + 1
			renameArrayItem(keytypevals, 'timestamp', 'epoch')
			attrsInJson,typesInJson = toCommaStringDict(keytypevals)
			
			if (isJsonKey(jiv, 'screensize')):
				featureAttrs2 = {'height', 'width'}
				cntattr2 = 0
				keytypevals2 = {}
				values2 = []
				for tis2 in featureAttrs2:
					keytypevals2,values2 = appendJsonKey(jiv['screensize'], tis2, keytypevals2, values2, cntattr2)
					cntattr2 = cntattr2 + 1
				
				renameArrayItem(keytypevals2, 'height', 'screen_height')
				renameArrayItem(keytypevals2, 'width', 'screen_width')
				attrsInJson2,typesInJson2 = toCommaStringDict(keytypevals2)
			
			#combine
			attrsInJsonCombined = attrsInJson
			typesInJsonCombined = typesInJson
			if ( attrsInJson2 != ''):
						attrsInJsonCombined = attrsInJsonCombined + ',' + attrsInJson2
						typesInJsonCombined = typesInJsonCombined + ',' + typesInJson2
						values.extend(values2)

			dbinsert(tblName,attrsInJsonCombined,typesInJsonCombined,cursor,values,conn)

## Network File
def insertNetwork(filetype, json, cursor, conn, uid):
	if (filetype == 'network'):
		networkAttrs = {'BSSID', 'IP', 'MAC', 'RSSI', 'SSID', 'detailedState', 'extraInfo', 'frequency', 'hasInternet', 'linkSpeed', 'mobileStatus', 'netID', 'signalStrength', 'timestamp', 'wiMaxStatus', 'wifiStatus'}
		cnt = 0
		for jiv in json:
			tblName = 'network'
			cntattr = 0
			keytypevals = {}
			values = []
			for tis in networkAttrs:
				keytypevals,values = appendJsonKey(jiv, tis, keytypevals, values, cntattr)
				cntattr = cntattr + 1
			
			keytypevals[cntattr] =  'uid'
			cntattr = cntattr + 1
			values.append(uid)
			renameArrayItem(keytypevals, 'timestamp', 'epoch')
			attrsInJson,typesInJson = toCommaStringDict(keytypevals)
			dbinsert(tblName,attrsInJson,typesInJson,cursor,values,conn)
			
			if isJsonKey(jiv, 'availableNetworks'):
				for innerjiv in jiv['availableNetworks']:
					innernetworkAttrs = {'BSSID', 'SSID', 'capabilities'}
					tblNameinner = 'network_availableNetwork'
					innercount = 0
					keytypevalsinner = {}
					valuesinner = []
					cntattrinner = 0
					for tisinner in innernetworkAttrs:
						keytypevalsinner,valuesinner = appendJsonKey(innerjiv, tisinner, keytypevalsinner, valuesinner, cntattrinner)
						cntattrinner = cntattrinner + 1
					maxID = getMaxId(tblName, cursor, conn)
					keytypevalsinner[cntattrinner] =  'network_id'
					cntattrinner = cntattrinner + 1
					valuesinner.append(maxID)
					attrsInJsoninner,typesInJsoninner = toCommaStringDict(keytypevalsinner)
					dbinsert(tblNameinner,attrsInJsoninner,typesInJsoninner,cursor,valuesinner,conn)
			
			if isJsonKey(jiv, 'capabilities'):
				for innerjiv2 in jiv['capabilities']:
					try:
						innernetworkAttrs2 = {'mLinkDownBandwidthKbps', 'mLinkUpBandwidthKbps', 'mNetworkCapabilities', 'mSignalStrength', 'mTransportTypes'}
						tblNameinner2 = 'network_capabilities'
						innercount2 = 0
						keytypevalsinner2 = {}
						valuesinner2 = []
						cntattrinner2 = 0
						for tisinner2 in innernetworkAttrs2:
							keytypevalsinner2,valuesinner2 = appendJsonKey(innerjiv2, tisinner2, keytypevalsinner2, valuesinner2, cntattrinner2)
							cntattrinner2 = cntattrinner2 + 1
						maxID2 = getMaxId(tblName, cursor, conn)
						keytypevalsinner2[cntattrinner2] =  'network_id'
						cntattrinner2 = cntattrinner2 + 1
						valuesinner2.append(maxID2)
						attrsInJsoninner2,typesInJsoninner2 = toCommaStringDict(keytypevalsinner2)
						dbinsert(tblNameinner2,attrsInJsoninner2,typesInJsoninner2,cursor,valuesinner2,conn)
					except:
						dummy = 0
			
			if isJsonKey(jiv, 'linkProperties'):
				for innerjiv3 in jiv['linkProperties']:
					try:
						innernetworkAttrs3 = {'mDomains', 'mIfaceName', 'mMtu', 'mTcpBufferSizes'}
						innernetworkAttrs3b = {'mDnses'}
						tblNameinner3 = 'network_linkProperties'
						innercount3 = 0
						keytypevalsinner3 = {}
						valuesinner3 = []
						cntattrinner3 = 0
						for tisinner3 in innernetworkAttrs3:
							keytypevalsinner3,valuesinner3 = appendJsonKey(innerjiv3, tisinner3, keytypevalsinner3, valuesinner3, cntattrinner3)
							cntattrinner3 = cntattrinner3 + 1
						keytypevalsinner3b = {}
						valuesinner3b = []
						for tisinner3b in innernetworkAttrs3b:
							keytypevalsinner3b,valuesinner3b = appendJsonKeyConcat(innerjiv3, tisinner3b, keytypevalsinner3b, valuesinner3b, cntattrinner3)
							cntattrinner3 = cntattrinner3 + 1
						maxID3 = getMaxId(tblName, cursor, conn)
						keytypevalsinner3[cntattrinner3] =  'network_id'
						cntattrinner3 = cntattrinner3 + 1
						valuesinner3.append(maxID3)
						attrsInJsoninner3,typesInJsoninner3 = toCommaStringDict(keytypevalsinner3)
						attrsInJsoninner3b,typesInJsoninner3b = toCommaStringDict(keytypevalsinner3b)
						#combine
						attrsInJsonCombined = attrsInJsoninner3
						typesInJsonCombined = typesInJsoninner3
						if ( attrsInJsoninner3b != ''):
							attrsInJsonCombined = attrsInJsonCombined + ',' + attrsInJsoninner3b
							typesInJsonCombined = typesInJsonCombined + ',' + typesInJsoninner3b
							valuesinner3.extend(valuesinner3b)
						dbinsert(tblNameinner3,attrsInJsonCombined,typesInJsonCombined,cursor,valuesinner3,conn)
						
						if isJsonKey(innerjiv3, 'mLinkAddresses'):
							for innerjiv5 in innerjiv3['mLinkAddresses']:
								innernetworkAttrs5 = {'address', 'flags', 'prefixLength', 'scope'}
								tblNameinner5 = 'network_linkProperties_mLinkAddresses'
								keytypevalsinner5 = {}
								valuesinner5 = []
								cntattrinner5 = 0
								for tisinner5 in innernetworkAttrs5:
									keytypevalsinner5,valuesinner5 = appendJsonKey(innerjiv5, tisinner5, keytypevalsinner5, valuesinner5, cntattrinner5)
									cntattrinner5 = cntattrinner5 + 1
								maxID5 = getMaxId(tblNameinner3, cursor, conn)
								keytypevalsinner5[cntattrinner5] =  'network_linkProperties_id'
								cntattrinner5 = cntattrinner5 + 1
								valuesinner5.append(maxID5)
								attrsInJsoninner5,typesInJsoninner5 = toCommaStringDict(keytypevalsinner5)
								dbinsert(tblNameinner5,attrsInJsoninner5,typesInJsoninner5,cursor,valuesinner5,conn)
						
						if isJsonKey(innerjiv3, 'mRoutes'):
							for innerjiv6 in innerjiv3['mRoutes']:
								innernetworkAttrs6 = {'mGateway', 'mHasGateway', 'mInterface', 'mIsHost', 'mType'}
								tblNameinner6 = 'network_linkProperties_mRoutes'
								keytypevalsinner6 = {}
								valuesinner6 = []
								cntattrinner6 = 0
								for tisinner6 in innernetworkAttrs6:
									keytypevalsinner6,valuesinner6 = appendJsonKey(innerjiv6, tisinner6, keytypevalsinner6, valuesinner6, cntattrinner6)
									cntattrinner6 = cntattrinner6 + 1
								maxID6 = getMaxId(tblNameinner3, cursor, conn)
								keytypevalsinner6[cntattrinner6] =  'network_linkProperties_id'
								cntattrinner6 = cntattrinner6 + 1
								valuesinner6.append(maxID6)
								attrsInJsoninner6,typesInJsoninner6 = toCommaStringDict(keytypevalsinner6)
								
								dbinsert(tblNameinner6,attrsInJsoninner6,typesInJsoninner6,cursor,valuesinner6,conn)
					except:
						dummy = 0
			cnt = cnt + 1

## Sensor File	(temperature and humidity not coded)
def insertSensor(filetype, json, cursor, conn, session):

	if (filetype == 'sensors' and json['sensor'] == 'Location'):
		tblName = 'sensor_location'
		fields = 'uid, epoch, configAccuracy'
		fieldTypes = '%s, %s, %s'
		values = [json['uid'], json['timestamp'], json['sensorData']['configAccuracy']]
		dbinsert(tblName,fields,fieldTypes,cursor,values,conn)
		maxID = getMaxId(tblName, cursor, conn)
		cnt = 0
		for jiv in json['sensorData']['locations']:
			tblName = 'sensor_location_data'
			fields = 'sensor_location_id, latitude, longitude, accuracy, speed, bearing, provider, time, local_time'
			fieldTypes = '%s, %s, %s, %s, %s, %s, %s, %s, %s'
			values = [maxID, 
			json['sensorData']['locations'][cnt]['latitude'], 
			json['sensorData']['locations'][cnt]['longitude'], 
			json['sensorData']['locations'][cnt]['accuracy'], 
			json['sensorData']['locations'][cnt]['speed'], 
			json['sensorData']['locations'][cnt]['bearing'],
			json['sensorData']['locations'][cnt]['provider'],
			json['sensorData']['locations'][cnt]['time'],
			json['sensorData']['locations'][cnt]['local_time']]
			dbinsert(tblName,fields,fieldTypes,cursor,values,conn)
			cnt = cnt + 1	

	if (filetype == 'sensors' and json['sensor'] == 'WiFi'):
		tblName = 'sensor_WiFi'
		fields = 'uid, epoch, senseCycles'
		fieldTypes = '%s, %s, %s'
		values = [json['uid'], json['timestamp'], json['sensorData']['senseCycles']]
		dbinsert(tblName,fields,fieldTypes,cursor,values,conn)
		maxID = getMaxId(tblName, cursor, conn)
		cnt = 0
		for jiv in json['sensorData']['scanResult']:
			tblName = 'sensor_WiFi_scanResult'
			fields = 'sensor_WiFi_id, ssid, bssid, capabilities, level, frequency'
			fieldTypes = '%s, %s, %s, %s, %s, %s'
			values = [maxID, 
			json['sensorData']['scanResult'][cnt]['ssid'], 
			json['sensorData']['scanResult'][cnt]['bssid'], 
			json['sensorData']['scanResult'][cnt]['capabilities'], 
			json['sensorData']['scanResult'][cnt]['level'], 
			json['sensorData']['scanResult'][cnt]['frequency'] ]
			dbinsert(tblName,fields,fieldTypes,cursor,values,conn)
			cnt = cnt + 1	

	if (filetype == 'sensors' and json['sensor'] == 'MagneticField'):
		tblName = 'betterXkeyspace.sensor_magneticfield'
		cnt = 0
		for jiv in json['sensorData']['sensorTimeStamps']:
			fields = 'uid, xAxis, yAxis, zAxis, epoch'
			fieldTypes = '%s, %s, %s, %s, %s'
			values = [json['uid'], json['sensorData']['xAxis'][cnt], json['sensorData']['yAxis'][cnt], json['sensorData']['zAxis'][cnt], json['sensorData']['sensorTimeStamps'][cnt] ]
			cassandraInsert(tblName,fields,fieldTypes,values,session)
			cnt = cnt + 1

	if (filetype == 'sensors' and json['sensor'] == 'Gyroscope'):
		tblName = 'betterXkeyspace.sensor_gyroscope'
		cnt = 0
		for jiv in json['sensorData']['sensorTimeStamps']:
			fields = 'uid, xAxis, yAxis, zAxis, epoch'
			fieldTypes = '%s, %s, %s, %s, %s'
			values = [json['uid'], 
			json['sensorData']['xAxis'][cnt], 
			json['sensorData']['yAxis'][cnt],
			json['sensorData']['zAxis'][cnt], 
			json['sensorData']['sensorTimeStamps'][cnt] ]
			cassandraInsert(tblName,fields,fieldTypes,values,session)
			cnt = cnt + 1

	if (filetype == 'sensors' and json['sensor'] == 'Light'):
		tblName = 'betterXkeyspace.sensor_light'
		fields = 'uid, epoch, light, maxRange'
		fieldTypes = '%s, %s, %s, %s' 
		values = [json['uid'], json['timestamp'], json['sensorData']['light'],json['sensorData']['maxRange'] ]
		cassandraInsert(tblName,fields,fieldTypes,values,session)
	
	if (filetype == 'sensors' and json['sensor'] == 'Battery'):
		tblName = 'sensor_battery'
		fields = 'uid, epoch, level, scale, temp, voltage, plugged, status, health'
		fieldTypes = '%s, %s, %s, %s, %s, %s, %s, %s, %s' 
		values = [json['uid'], json['timestamp'], json['sensorData']['level'],json['sensorData']['scale'],json['sensorData']['temperature'],json['sensorData']['voltage'], json['sensorData']['plugged'],json['sensorData']['status'], json['sensorData']['health'] ]
		dbinsert(tblName,fields,fieldTypes,cursor,values,conn)
	
	if (filetype == 'sensors' and json['sensor'] == 'Connection'):
		tblName = 'sensor_connection'
		fields = 'uid, epoch, connected, connecting, available, networkType, roaming, ssid'
		fieldTypes = '%s, %s, %s, %s, %s, %s, %s, %s' 
		try:
			values = [json['uid'], json['timestamp'], json['sensorData']['connected'],json['sensorData']['connecting'],json['sensorData']['available'],json['sensorData']['networkType'], json['sensorData']['roaming'],json['sensorData']['ssid'] ]
		except:
			values = [json['uid'], json['timestamp'], json['sensorData']['connected'],json['sensorData']['connecting'],json['sensorData']['available'],json['sensorData']['networkType'], json['sensorData']['roaming'], None ]
		dbinsert(tblName,fields,fieldTypes,cursor,values,conn)
	
	if (filetype == 'sensors' and json['sensor'] == 'ConnectionStrength'):
		tblName = 'sensor_connectionStrength'
		fields = 'uid, epoch, strength'
		fieldTypes = '%s, %s, %s' 
		values = [json['uid'], json['timestamp'], json['sensorData']['strength']]
		dbinsert(tblName,fields,fieldTypes,cursor,values,conn)
	
	if (filetype == 'sensors' and json['sensor'] == 'PassiveLocation'):
		tblName = 'sensor_passiveLocation'
		fields = 'uid, epoch, latitude, longitude, accuracy, speed, bearing, provider, time'
		fieldTypes = '%s, %s, %s, %s, %s, %s, %s, %s, %s'
		values = [json['uid'], json['timestamp'], json['sensorData']['latitude'], 
		json['sensorData']['longitude'], json['sensorData']['accuracy'], json['sensorData']['speed'],
		json['sensorData']['bearing'], json['sensorData']['provider'], json['sensorData']['time']]
		dbinsert(tblName,fields,fieldTypes,cursor,values,conn)

	if (filetype == 'sensors' and json['sensor'] == 'PhoneState'):
		tblName = 'sensor_phoneState'
		fields = 'uid, epoch, eventType, data'
		fieldTypes = '%s, %s, %s, %s'
		values = [json['uid'], json['timestamp'], json['sensorData']['eventType'],json['sensorData']['data']]
		dbinsert(tblName,fields,fieldTypes,cursor,values,conn)	
		
	if (filetype == 'sensors' and json['sensor'] == 'Screen'):
		tblName = 'sensor_screen'
		fields = 'uid, epoch, status'
		fieldTypes = '%s, %s, %s'
		values = [json['uid'], json['timestamp'], json['sensorData']['status']]
		dbinsert(tblName,fields,fieldTypes,cursor,values,conn)		
		
	if (filetype == 'sensors' and json['sensor'] == 'StepCounter'):
		tblName = 'sensor_stepCounter'
		fields = 'uid, epoch, stepCount'
		fieldTypes = '%s, %s, %s'
		values = [json['uid'], json['timestamp'], json['sensorData']['stepCount']]
		dbinsert(tblName,fields,fieldTypes,cursor,values,conn)		
		
	if (filetype == 'sensors' and json['sensor'] == 'Accelerometer'):
		tblName = 'betterXkeyspace.sensor_accelerometer'
		cnt = 0
		for jiv in json['sensorData']['sensorTimeStamps']:
			fields = 'uid, xAxis, yAxis, zAxis, epoch'
			fieldTypes = '%s, %s, %s, %s, %s'
			values = [json['uid'], 
			json['sensorData']['xAxis'][cnt], 
			json['sensorData']['yAxis'][cnt],
			json['sensorData']['zAxis'][cnt], 
			json['sensorData']['sensorTimeStamps'][cnt] ]
			cassandraInsert(tblName,fields,fieldTypes,values,session)
			cnt = cnt + 1


## Helper Functions	

def cassandraInsert(tblName, fields, fieldTypes, valuesList, session):
	jiv = 0
	session.execute_async("INSERT INTO " + tblName + " (" + fields + ") VALUES ("+fieldTypes + ")", valuesList)
					
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