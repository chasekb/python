##datapull2
##Kahlil Bernard Chase

##
##objectives
##1. transform existing snp500 txt databases to sqlite
##2. join each snp500 component sqlite database into snp500 database
##3. pull data and save to snp500.db

import time
import datetime
import sqlite3
from contextlib import suppress
from urllib.request import urlopen

def pullData1d(stock):
    with suppress(Exception):
        print('Currently pulling ' + stock + ' minute closes')
        print(str(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')))
        urlToVisit = 'http://chartapi.finance.yahoo.com/instrument/1.0/'+stock+'/chartdata;type=quote;range=1d/csv'
        saveFileLine = 'snp500/'+stock+'1d.txt'

        try:
            readExistingData = open(saveFileLine,'r').read()
            splitExisting = readExistingData.split('\n')
            mostRecentLine = splitExisting[-2]
            lastUnix = mostRecentLine.split(',')[0]            
        except:
            lastUnix = 0

        saveFile = open(saveFileLine,'a')
        sourceCode = urlopen(urlToVisit).read()
        sourceDecode = sourceCode.decode('utf-8')
        splitSource = sourceDecode.split('\n')

        for eachLine in splitSource:
            if 'values' not in eachLine:
                splitLine = eachLine.split(',')
                if len(splitLine)==6:
                    if int(splitLine[0]) > int(lastUnix):
                        lineToWrite = eachLine+'\n'
                        saveFile.write(lineToWrite)

        saveFile.close()

        print('Pulled ' + stock + ' minute closes')
        print(str(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')))

def pullData1y(stock):
    with suppress(Exception):
        print('Currently pulling ' + stock + ' daily closes')
        print(str(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')))
        urlToVisit = 'http://chartapi.finance.yahoo.com/instrument/1.0/'+stock+'/chartdata;type=quote;range=1y/csv'
        saveFileLine = 'snp500/'+stock+'1y.txt'

        try:
            readExistingData = open(saveFileLine,'r').read()
            splitExisting = readExistingData.split('\n')
            mostRecentLine = splitExisting[-2]
            lastUnix = mostRecentLine.split(',')[0]            
        except:
            lastUnix = 0

        saveFile = open(saveFileLine,'a')
        sourceCode = urlopen(urlToVisit).read()
        sourceDecode = sourceCode.decode('utf-8')
        splitSource = sourceDecode.split('\n')

        for eachLine in splitSource:
            if 'values' not in eachLine:
                splitLine = eachLine.split(',')
                if len(splitLine)==6:
                    if int(splitLine[0]) > int(lastUnix):
                        lineToWrite = eachLine+'\n'
                        saveFile.write(lineToWrite)

        saveFile.close()

        print('Pulled ' + stock + ' daily closes')
        print(str(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')))

stocksToPullList = open('snp500.txt','r').read()
stocksToPull = stocksToPullList.split('\n')

while True:
    for eachStock in stocksToPull:
        pullData1d(eachStock)
        pullData1y(eachStock)
        print('closing...')

    break

'''    
    time.sleep(18000)
'''
