#!/usr/bin/python
# coding: utf8

import os, urllib, hmac, binascii, base64, hashlib, urllib2, json, pprint, simplejson, sys, time, csv
import MySQLdb
from optparse import OptionParser
import codecs

from warnings import filterwarnings
filterwarnings('ignore', category = MySQLdb.Warning)

host = ''
user = ''
password = ''
db = ''
path = ''
mainquery = '''SELECT 
betterX._user_webSessions.uid as uid, 
dayofyear(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `dayOfYear`, count(*) as cnt
FROM betterX._user_webSessions
left join betterX.setup on betterX.setup.uid = betterX._user_webSessions.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where dayofyear(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) is not null
group by betterX._user_webSessions.uid, dayofyear(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by count(*) desc'''

locationsQuery = """SELECT betterX._user_webSessions.userTimestamp as `timestamp`, a.latitude as lat, a.longitude as lon,
replace(
	CONCAT(
		'<strong>', betterX._user_webSessions.location_address, '</strong>', '<br/>', 'Accuracy:', cast(a.accuracy as char(10)), 
		'<br/>', 'LocationSessions: ' , cast(b.CntLocationSessions as char(10)),
		'<br/>', 'TodaysLocationSessions: ', cast(c.CntTodaysLocationSessions as char(10)),
		'<br/>', 'LocationDomains: ', cast(b.CntLocationDomains as char(10)), 
		'<br/>', 'TodaysLocationDomains: ', cast(c.CntTodaysLocationDomains as char(10)),
		'<br/>', 'LocationDomainList: ', cast(b.LocationDomainsList as char(250)),
        '<br/>', 'TodaysLocationDomainList: ', cast(c.TodaysLocationDomainsList as char(250))
        ) collate utf8_unicode_ci
	,
	',', '|') collate utf8_unicode_ci as `desc` 
FROM betterX._user_webSessions 
left join
	(
		select _user_geolocations.address, min(_user_locations.accuracy) as accuracy, max(_user_geolocations.latitude) as latitude, max(_user_geolocations.longitude) as longitude
        from _user_geolocations
        left join _user_locations on _user_locations.latitude = _user_geolocations.latitude and _user_locations.longitude = _user_geolocations.longitude
        where _user_geolocations.address is not null and _user_geolocations.latitude is not null and _user_geolocations.longitude is not null
        group by address
    ) as a on a.address = betterX._user_webSessions.location_address
left join 
	(
    select uid, location_address, count(*) as CntLocationSessions, count(distinct(domain)) as CntLocationDomains, GROUP_CONCAT(distinct(domain)) as LocationDomainsList
	from betterX._user_webSessions
	where location_address is not null
	group by uid, location_address
	order by uid, location_address
    ) as b on b.uid = betterX._user_webSessions.uid and b.location_address = betterX._user_webSessions.location_address
left join
	(
	select uid, dayofyear(betterX._user_webSessions.userTime) as `day`, location_address, count(*) as CntTodaysLocationSessions, count(distinct(domain)) as CntTodaysLocationDomains, GROUP_CONCAT(distinct(domain)) as TodaysLocationDomainsList
	from betterX._user_webSessions
	where location_address is not null and betterX._user_webSessions.userTime is not null
	group by uid, dayofyear(betterX._user_webSessions.userTime), location_address
	order by uid, dayofyear(betterX._user_webSessions.userTime), location_address
    ) as c on c.uid = betterX._user_webSessions.uid and c.location_address = betterX._user_webSessions.location_address and c.`day` = dayofyear(betterX._user_webSessions.userTime)
where betterX._user_webSessions.uid = @userid
and dayofyear(betterX._user_webSessions.userTime) = @userday 
and betterX._user_webSessions.location_address is not null and a.latitude is not null and a.longitude is not null
group by `timestamp`, lat, lon, `desc`
order by betterX._user_webSessions.userTimestamp asc 
"""

metricsList = [ 
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'pageTitle as `value`','name_pageTitle'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'pageOnContentLoad as `value`','time_pageOnContentLoad'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'pageOnLoad as `value`','time_pageOnLoad'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'origin as `value`','origin'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'time as `value`','time_total'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'connection as `value`','time_connection'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'blocked as `value`','time_blocked'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'dns as `value`','time_dns'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'connect as `value`','time_connect'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'send as `value`','time_send'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'wait as `value`','time_wait'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'receive as `value`','time_receive'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'`ssl` as `value`','time_ssl'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'beforeRequestCacheEntries as `value`','cache_beforeRequestCacheEntries'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'afterRequestCacheEntries as `value`','cache_afterRequestCacheEntries'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'hitCount as `value`','cache_hitCount'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'method as `value`','http_method'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'url as `value`','name_url'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'httpVersionRequest as `value`','http_httpVersionRequest'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'cookieNumberRequest as `value`','cookie_cookieNumberRequest'],
#["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'header_UserAgent as `value`','header_UserAgent'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'header_Accept as `value`','type_header_Accept'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'header_AcceptEncoding as `value`','encoding_header_AcceptEncoding'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'header_ConnectionRequest as `value`','connection_header_ConnectionRequest'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'header_ContentLengthRequest as `value`','size_header_ContentLengthRequest'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'header_KeepAliveRequest as `value`','connection_header_KeepAliveRequest'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'headerSize as `value`','size_headerSize'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'bodySizeRequest as `value`','size_bodySizeRequest'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'status as `value`','http_status'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'statusText as `value`','http_statusText'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'httpVersionResponse as `value`','http_httpVersionResponse'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'cookieNumberResponse as `value`','cookie_cookieNumberResponse'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'cookieNumberRequest as `value`','cookie_cookieNumberRequest'],
#["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'header_Server as `value`','header_Server'],
#["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'header_XPoweredBy as `value`','header_XPoweredBy'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'header_ContentEncoding as `value`','encoding_header_ContentEncoding'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'header_ContentLengthResponse as `value`','size_header_ContentLengthResponse'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'header_KeepAliveResponse as `value`','connection_header_KeepAliveResponse'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'header_ConnectionResponse as `value`','connection_header_ConnectionResponse'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'header_ContentType as `value`','type_header_ContentType'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'redirectUrl as `value`','redirect_redirectUrl'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'headersSize as `value`','size_headersSize'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'bodySizeResponse as `value`','size_bodySizeResponse'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'content_size as `value`','size_content_size'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'content_compression as `value`','type_content_compression'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'content_mimeType as `value`','type_content_mimeType'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'content_encoding as `value`','encoding_content_encoding'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'domain as `value`','name_domain'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'title as `value`','name_title'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'Speed_MediaLoadingTime as `value`','time_Speed_MediaLoadingTime'],
#["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'Speed_Percentile as `value`','Speed_Percentile'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'Category1 as `value`','name_Category1'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'durationTab as `value`','time_durationTab'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'estimateDuration as `value`','time_estimateDuration'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'location_address as `value`','location_location_address'],
["UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`",'location_accuracy as `value`','location_location_accuracy']
]

readingsList = [
["apps", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.apps.`timestamp` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
replace(GROUP_CONCAT(distinct(app)), ',', '|') as `value`
FROM betterX.apps
left join betterX.setup on betterX.setup.uid = betterX.apps.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.apps.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.apps.`timestamp` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.apps.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.apps.`timestamp` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.apps.`timestamp` asc
"""],
["battery_level",
"""select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_battery.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
MIN(`level`) as `value`
FROM betterX.sensor_battery
left join betterX.setup on betterX.setup.uid = betterX.sensor_battery.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.sensor_battery.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_battery.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.sensor_battery.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_battery.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.sensor_battery.`epoch` asc
"""],
["battery_temp","""
select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_battery.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
MIN(`temp`) as `value`
FROM betterX.sensor_battery
left join betterX.setup on betterX.setup.uid = betterX.sensor_battery.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.sensor_battery.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_battery.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.sensor_battery.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_battery.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.sensor_battery.`epoch` asc
"""],
["connection_connected", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
cast(MIN(`connected`) as unsigned) as `value`
FROM betterX.sensor_connection
left join betterX.setup on betterX.setup.uid = betterX.sensor_connection.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.sensor_connection.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.sensor_connection.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.sensor_connection.`epoch` asc
"""],
["connection_connecting", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
cast(MIN(`connecting`) as unsigned) as `value`
FROM betterX.sensor_connection
left join betterX.setup on betterX.setup.uid = betterX.sensor_connection.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.sensor_connection.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.sensor_connection.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.sensor_connection.`epoch` asc
"""],
["connection_available", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
cast(MIN(`available`) as unsigned) as `value`
FROM betterX.sensor_connection
left join betterX.setup on betterX.setup.uid = betterX.sensor_connection.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.sensor_connection.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.sensor_connection.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.sensor_connection.`epoch` asc
"""],
["connection_networkType", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
replace(GROUP_CONCAT(DISTINCT(`networkType`)), ',', '|') as `value`
FROM betterX.sensor_connection
left join betterX.setup on betterX.setup.uid = betterX.sensor_connection.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.sensor_connection.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.sensor_connection.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.sensor_connection.`epoch` asc
"""],
["connection_roaming", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
replace(GROUP_CONCAT(DISTINCT(`roaming`)), ',', '|') as `value`
FROM betterX.sensor_connection
left join betterX.setup on betterX.setup.uid = betterX.sensor_connection.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.sensor_connection.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.sensor_connection.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connection.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.sensor_connection.`epoch` asc
"""],
["connection_strength", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connectionStrength.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
MIN(`strength`) as `value`
FROM betterX.sensor_connectionStrength
left join betterX.setup on betterX.setup.uid = betterX.sensor_connectionStrength.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.sensor_connectionStrength.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connectionStrength.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.sensor_connectionStrength.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_connectionStrength.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.sensor_connectionStrength.`epoch` asc
"""],
["network_ssid", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
replace(GROUP_CONCAT(DISTINCT(`ssid`)), ',', '|') as `value`
FROM betterX.network
left join betterX.setup on betterX.setup.uid = betterX.network.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.network.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.network.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.network.`epoch` asc
"""],
["network_state", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
replace(GROUP_CONCAT(DISTINCT(`detailedState`)), ',', '|') as `value`
FROM betterX.network
left join betterX.setup on betterX.setup.uid = betterX.network.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.network.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.network.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.network.`epoch` asc
"""],
["network_internet", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
replace(GROUP_CONCAT(DISTINCT(`hasInternet`)), ',', '|') as `value`
FROM betterX.network
left join betterX.setup on betterX.setup.uid = betterX.network.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.network.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.network.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.network.`epoch` asc
"""],
["network_linkSpeed", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
MIN(`linkSpeed`) as `value`
FROM betterX.network
left join betterX.setup on betterX.setup.uid = betterX.network.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.network.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.network.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.network.`epoch` asc
"""],
["network_mobile", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
replace(GROUP_CONCAT(DISTINCT(`mobileStatus`)), ',', '|') as `value`
FROM betterX.network
left join betterX.setup on betterX.setup.uid = betterX.network.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.network.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.network.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.network.`epoch` asc
"""],
["network_signalStrength", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
MIN(`signalStrength`) as `value`
FROM betterX.network
left join betterX.setup on betterX.setup.uid = betterX.network.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.network.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.network.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.network.`epoch` asc
"""],
["network_wifi", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
replace(GROUP_CONCAT(DISTINCT(`wifiStatus`)), ',', '|') as `value`
FROM betterX.network
left join betterX.setup on betterX.setup.uid = betterX.network.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.network.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.network.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.network.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.network.`epoch` asc
"""],
["phoneState", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_phoneState.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
replace(GROUP_CONCAT(DISTINCT(
trim(CONCAT(
if (left(`data`, 10) = 'CALL_STATE', trim(`data`), ''),
if (left(`data`, 13) = 'DATA_ACTIVITY', trim(`data`), ''),
if (left(`data`, 14) = 'DATA_CONNECTED', trim(`data`), ''),
if (left(`data`, 17) = 'DATA_DISCONNECTED', trim(`data`), ''),
if (left(`data`, 14) = 'DATA_SUSPENDED', trim(`data`), ''),
if (left(`data`, 20) = 'STATE_EMERGENCY_ONLY', trim(left(`data`, locate(' ', `data`))), ''),
if (left(`data`, 16) = 'STATE_IN_SERVICE', trim(left(`data`, locate(' ', `data`))), ''),
if (left(`data`, 20) = 'STATE_OUT_OF_SERVICE', trim(left(`data`, locate(' ', `data`))), ''),
if (left(`data`, 15) = 'STATE_POWER_OFF', trim(left(`data`, locate(' ', `data`))), '')
)) COLLATE utf8_unicode_ci)), ',' , '|') as `value`
FROM betterX.sensor_phoneState
left join betterX.setup on betterX.setup.uid = betterX.sensor_phoneState.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.sensor_phoneState.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_phoneState.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.sensor_phoneState.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_phoneState.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.sensor_phoneState.`epoch` asc
"""],
["phoneScreen", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_screen.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
replace(GROUP_CONCAT(DISTINCT(`status`)), ',', '|') as `value`
FROM betterX.sensor_screen
left join betterX.setup on betterX.setup.uid = betterX.sensor_screen.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.sensor_screen.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_screen.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.sensor_screen.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_screen.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.sensor_screen.`epoch` asc
"""],
["steps", """select
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_stepCounter.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) as `timestamp`,
MAX(stepCount) as `value`
FROM betterX.sensor_stepCounter
left join betterX.setup on betterX.setup.uid = betterX.sensor_stepCounter.uid 
left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone
where betterX.sensor_stepCounter.uid = @userid 
and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_stepCounter.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = @userday
group by betterX.sensor_stepCounter.uid, 
UNIX_TIMESTAMP(CONVERT_TZ(from_unixtime( cast(cast(betterX.sensor_stepCounter.`epoch` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr))
order by  betterX.sensor_stepCounter.`epoch` asc
"""]
]


try:
	conn = MySQLdb.connect (host, user, password, db, charset='utf8')
except:
	print ">> Unable to connect...exit"
	sys.exit()
else:
	cursor = conn.cursor()
	cursor.execute (mainquery)
	rows = cursor.fetchall()
	numrows = int(cursor.rowcount)
	mainFile = codecs.open(path + 'index.csv' , 'wb', 'utf-8')
	mainFile.write('userid,fileTypeName,dayofYear' + "\n")
	for i in range(numrows):
		userID = str(rows[i][0])
		userDay =  rows[i][1]
		userRecordCount =  rows[i][2]
		for j in metricsList:
			attr1 = str(j[0])
			attr2 = str(j[1])
			fileName = str(j[2])
			q = "select " + attr1 + "," + attr2 + " from betterX._user_webSessions left join betterX.setup on betterX.setup.uid = betterX._user_webSessions.uid  left join betterX._user_timeZones on betterX._user_timeZones.tzone_raw = betterX.setup.timezone where betterX._user_webSessions.uid = '" + userID + "' and dayofyear(CONVERT_TZ(from_unixtime( cast(cast(`pageStartTime` as char(10)) as unsigned) ), 'UTC', betterX._user_timeZones.tzone_abbr)) = " + str(userDay) + " order by pageStartTime asc"
			cursor.execute(q)
			rows2 = cursor.fetchall()
			numrows2 = int(cursor.rowcount)
			if (numrows2 > 0):
				mainFile.write(str(userID) + "," + str(fileName) + "," + str(userDay) + "\n")
				myfile = codecs.open(path + userID + '_' + fileName + '_' + str(userDay) + '.csv' , 'wb', 'utf-8')
				myfile.write('timestamp' + "," + 'value' + "\n")
				for i2 in range(numrows2):
					try:
						enc = str(str(rows2[i2][0]) + "," + str(rows2[i2][1])).encode("utf-8")
					except:
						enc = ' '
					
					myfile.write(enc + "\n")
				myfile.close()
		for k in readingsList:
			readingsName = str(k[0])
			readingsSQL = str(k[1])
			readingsSQL = readingsSQL.replace("@userid", str("'" + userID + "'"))
			readingsSQL = readingsSQL.replace("@userday", str(userDay))
			cursor.execute(readingsSQL)
			rowsK = cursor.fetchall()
			numrowsK = int(cursor.rowcount)
			if (numrowsK > 0):
				mainFile.write(str(userID) + "," + readingsName + "," + str(userDay) + "\n")
				myfile = codecs.open(path + userID + '_' + readingsName + '_' + str(userDay) + '.csv' , 'wb', 'utf-8')
				myfile.write('timestamp' + "," + 'value' + "\n")
				for iK in range(numrowsK):
					try:
						enc = str(str(rowsK[iK][0]) + "," + str(rowsK[iK][1])).encode("utf-8")
					except:
						enc = ' '
					myfile.write(enc + "\n")
				myfile.close()
		qL = locationsQuery.replace("@userid", str("'" + userID + "'"))
		qL = qL.replace("@userday", str(userDay))
		cursor.execute(qL)
		rowsL = cursor.fetchall()
		numrowsL = int(cursor.rowcount)
		if (numrowsL > 0):
			mainFile.write(str(userID) + "," + 'locations' + "," + str(userDay) + "\n")
			myfileL = codecs.open(path + userID + '_' + 'locations' + '_' + str(userDay) + '.csv' , 'wb', 'utf-8')
			myfileL.write('timestamp,lat,lon,desc' + "\n")
			for iL in range(numrowsL):
					try:
						encL = str(str(rowsL[iL][0]) + "," + str(rowsL[iL][1]) + "," + str(rowsL[iL][2]) + "," + str(rowsL[iL][3])).encode("utf-8")
					except:
						encL = ' '
					myfileL.write(encL + "\n")
			myfileL.close()
		#break;
	mainFile.close()
	cursor.close()
	conn.close()