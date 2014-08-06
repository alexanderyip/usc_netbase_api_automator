# -*- coding: utf-8 -*-
"""
Created on Wed Jan 08 15:05:32 2014

@author: ayip
"""
import requests, pyodbc, time, simplejson as json
from requests.auth import HTTPBasicAuth
from datetime import date, timedelta
db_user = 'XX'
db_pw = 'XX'
#conn_str = 'DRIVER={SQL Server};SERVER=madb;DATABASE=AnalyticsTestDB;UID='+db_user+';PWD='+db_pw
conn_str = 'DRIVER={SQL Server};SERVER=madb;DATABASE=AnalyticsTestDB;Trusted_Connection=yes'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

cursor.execute("select distinct [date] from [dbo].[USCellular_NB]")
date_rows = cursor.fetchall()
dates = []
for d in date_rows:
    dates.append(d[0])

user = 'XX'
pw = 'XX'

today = date.today()-timedelta(1)
endDate = date(2012,12,31)
#Topic Names are in Insights Composer
#these stopped @ 1/8/2013
#topicNames = ['USCC - Network','Att','Sprint','TMobile','Verizon']
topicNames = ['USCC - 2014']
metrics = ['TotalBuzz','TotalBuzzPost','PositiveSentiment','NegativeSentiment']
wait_counter = 0

while today > endDate:
    if today.isoformat() not in dates:
        for topicName in topicNames:
            print 'Working on '+topicName+' '+today.isoformat()
            # insert new date + topic into DB
            sql_line="insert into [dbo].[USCellular_NB] ([date],[topicName]) values ('"+today.isoformat()+"',"
            sql_line += "'"+topicName+"')"
            cursor.execute(sql_line)
            cursor.commit()
            # update db with info from NetBase
            request_line = 'https://api.netbase.com:443/cb/insight-api/2/metricValues?topics='
            request_line += topicName.replace(' ','%20')+'&metricSeries='
            request_line += '&metricSeries='.join(metrics)
            request_line += '&datetimeISO=true&pretty=false&timeUnits=All&publishedDate='
            request_line += today.isoformat()+'&publishedDate='+(today+timedelta(1)).isoformat()+'&precision=LOW&realTime=false&timePeriodOffset=0m&timePeriodRounding=1m'
            print request_line
            j = requests.get(request_line,auth=HTTPBasicAuth(user,pw))
            while j.status_code != 200:
                print str(j.status_code)+' '+j.reason
                # add a 1 minute wait to buffer API requests
                if wait_counter < 2:
                    print 'Waiting ~1 minute'
                    time.sleep(61)
                # if fail too many times, wait 30 min
                else:
                    print 'Waiting ~30 minutes'
                    time.sleep(1801)
                wait_counter += 1
                j = requests.get(request_line,auth=HTTPBasicAuth(user,pw))
            r = json.loads(j.text)
            wait_counter = 0
            for m in r['metrics'][0]['dataset']:
                print 'Metric: '+m['seriesName']
                sql_line = "update [dbo].[USCellular_NB] SET ["+m['seriesName']+"]"
                sql_line += "="+str(int(m['set'][0]))
                sql_line += " where [date]='"+today.isoformat()+"'"
                sql_line += " and [topicName]='"+topicName+"'"
                cursor.execute(sql_line)
                cursor.commit()
    else:
        print today.isoformat()+' already in database!'
    today = today - timedelta(1)
conn.close()
print 'Finished'
