import MySQLdb, socket
from urlparse import urlparse
import tldextract

dbhost = ''
dblogin = ''
dbpassword = ''
dbschema = ''
dbtable = ''


#main part
if __name__ == "__main__":
    connection = MySQLdb.connect(host=dbhost,user=dblogin, passwd=dbpassword, db=dbschema,charset='utf8', use_unicode=True)
    connection.autocommit(True)
    cursor = connection.cursor()
    cursor.execute('select * from `%s` where domain is null' % dbtable)
    fields = [v[0] for v in  cursor.description]
    idxID = fields.index('id')
    idxURL = fields.index('url')
    for row in cursor:
        ID = row[idxID]
        URL = row[idxURL]
        tis = urlparse(URL)
        if (tis.hostname != None):
			jiv = tldextract.extract(tis.hostname).domain + '.' + tldextract.extract(tis.hostname).suffix
			sql = 'UPDATE ' + dbtable + ' set domain = "' +  jiv + '" where id = ' +  str(ID)
			cursor.execute(sql)
			connection.commit()