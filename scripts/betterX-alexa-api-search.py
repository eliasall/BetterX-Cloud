import MySQLdb, socket, awis

dbhost = ''
dblogin = ''
dbpassword = ''
dbschema = ''
dbtable = ''

AWSAccessKeyID = ''
AWSSecretAccessKey = ''



#function to get data from AWS
api = awis.AwisApi(AWSAccessKeyID, AWSSecretAccessKey)
def getAWSData(domain):
    def getValueByXpath(tree,xpath):
        v = tree.xpath(xpath,namespaces=api.NS_PREFIXES)
        return v[0].encode('UTF-8') if len(v)>0 else None
    tree = api.url_info(domain, "SiteData", "Speed", "Categories")
    assert tree.xpath('//alexa:StatusCode/text()',namespaces=api.NS_PREFIXES)==['Success']
    result = {}
    result['Title'] = getValueByXpath(tree,'//awis:SiteData/awis:Title/text()')
    result['Description'] = getValueByXpath(tree,'//awis:SiteData/awis:Description/text()')
    result['Speed_MediaLoadingTime'] = getValueByXpath(tree,'//awis:Speed/awis:MedianLoadTime/text()')
    result['Speed_Percentile'] = getValueByXpath(tree,'//awis:Speed/awis:Percentile/text()')
    Categories = tree.xpath('//awis:Categories/awis:CategoryData/awis:AbsolutePath/text()',namespaces=api.NS_PREFIXES)[:3]
    result['Category1'] = Categories[0].encode('UTF-8') if len(Categories)>0 else None
    result['Category2'] = Categories[1].encode('UTF-8') if len(Categories)>1 else None
    result['Category3'] = Categories[2].encode('UTF-8') if len(Categories)>2 else None
    return result

#main part
if __name__ == "__main__":
    connection = MySQLdb.connect(host=dbhost,user=dblogin, passwd=dbpassword, db=dbschema,charset='utf8', use_unicode=True)
    connection.autocommit(True)
    cursor = connection.cursor()
    cursor.execute('select * from `%s` where `Title` IS NULL;' % dbtable)
    fields = [v[0] for v in  cursor.description]
    #idxIPAddress = fields.index('IPAddress')
    idxHost_Name = fields.index('Host_Name')
    for row in cursor:
        Host = row[idxHost_Name]
        print Host
        try:
            data = getAWSData(Host)
        except:
            continue
        updatingFields = data.keys()
        sql = 'UPDATE `{}` set {} where `Host_Name`=%s;'.format(dbtable,', '.join(['`{}`=%s'.format(field) for field in updatingFields]))
        cursor.execute(sql,tuple([data[field] for field in updatingFields]+[Host]))
        connection.commit()    
