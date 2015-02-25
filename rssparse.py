#!/usr/bin/env python
# -*- coding: utf-8 -*-

import feedparser
import json
import time
import calendar
from dateutil.parser import parse
import os
import urllib2
import sys
import csv
from pymongo import MongoClient

from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

db = MongoClient().feeds

urls = ['http://miamiherald.com/news/politics-government/index.rss']
fname = 'urls.csv'
#fname = 'except.csv'
with open(fname) as f:
    urls = f.readlines()
#urls = csv.reader(open('urls.csv', 'rb'));

feedparser.USER_AGENT = "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)"
#feedparser.USER_AGENT = "Mozilla/4.0 (compatible; SNATZ 0.9)"

debuglevel=0
h = urllib2.HTTPHandler(debuglevel)

def getpathname():
    path_name = time.strftime('%Y%m%d')
    return path_name

def href2path(href):
    href_name = href.replace('&', '#_#')
    href_name = href_name.replace('://', '__')
    href_name = href_name.replace('/', '_')
    return href_name

def writecontent(path_name, href_name, extension, content):
    try:
        if not os.path.exists(path_name):
            os.makedirs(path_name)
    except:
        #print "OS error:", sys.exc_info()[0]
        donothing=1

    myfile = open(path_name + os.sep + href_name + '=-=' + time.strftime('%H%M%S') + extension, 'wb+')
    myfile.write(content)
    myfile.close()


def writestamp(path_name, href_name, feed, html, badfeed):

    if 0==badfeed:
        e = enumerate(feed.entries)
        for y, element in e:
            if "published_parsed" in element:
                if element.published_parsed:
                    feed['entries'][y]['published_parsed'] = calendar.timegm(element.published_parsed)
            if "updated_parsed" in element:
                if element.updated_parsed:
                    feed['entries'][y]['updated_parsed'] = calendar.timegm(element.updated_parsed)
                    #del feed['entries'][y]['updated_parsed']
        json_string = json.dumps(feed.entries)
        path_name = path_name + os.sep +'GOOD'
        writecontent(path_name, href_name, '.json', json_string)
    else:
        path_name = path_name + os.sep +'BAD'

    writecontent(path_name, href_name, '.xml', html)


def writedbfeed(href, feed_header_modified, feed_header_code, feed_version):
    db_feed = db.feeds.find({'url':href})
    updated = int(time.time())
    db_feed_row = ''
    if feed_header_modified is not None:
        dt = parse(feed_header_modified)
        feed_header_modified = time.mktime(dt.timetuple())-dt.utcoffset().total_seconds()-time.timezone

    db_update_data = {'updated':updated, 'feed_header_modified':feed_header_modified, 'feed_header_code':feed_header_code, 'feed_version':feed_version}
    if db_feed.count() is not 0:
        for row in db_feed:
            db_feed_row = row

        db.feeds.update({'_id': db_feed_row['_id']}, {'$set' : db_update_data})
    else:
        db_update_data['url'] = href
        db.feeds.insert(db_update_data)


def getfeed(href):
    href = href.strip()
    path_name = getpathname()
    href_name = href2path(href)
    try:

        req = urllib2.Request(href)
        opener = urllib2.build_opener(h)
        opener.addheaders = [('User-Agent', feedparser.USER_AGENT), ('Accept', '*/*')]
        #opener.addheaders = [('User-Agent', feedparser.USER_AGENT)]
        urllib2.install_opener(opener)
        response = urllib2.urlopen(req, timeout=5)

        # response = opener.open(href, timeout=3)

        html = response.read()
        feed = feedparser.parse(html)
        # check that feed was parsed
        badfeed = 0
        if not feed.version:
            badfeed = 1

        writestamp(path_name, href_name, feed, html, badfeed)

        feed_header_modified = response.info().getheader('Last-Modified')
        feed_header_code = response.code
        feed_version = feed.version

        writedbfeed(href, feed_header_modified, feed_header_code, feed_version)
    except urllib2.HTTPError, ext:
        print href
        print "HTTPError", sys.exc_info()[0]
        print ext.reason, ext.code
        #print ext.read()
        writedbfeed(href, None, ext.code, 0)
        writecontent(path_name + os.sep + 'HTTPError', href_name, '.txt', 'please check ' + href)
        return 0
    except urllib2.URLError, ext:
        print href
        print "URLError", sys.exc_info()[0]
        writedbfeed(href, None, 0, 0)
        writecontent(path_name + os.sep + 'URLError', href_name, '.txt', 'please check ' + href)
        print ext.reason
        return 0
    except:
        print href
        writedbfeed(href, None, -1, 0)
        print "EXEPT", sys.exc_info()[0]
        writecontent(path_name + os.sep + 'EXEPT', href_name, '.txt', 'please check ' + href)
        return 0
    #print '= OK'
    return 1

# for row in urls:
#     row = row.strip()
#     print ""
#     print row
#     getfeed(row)
#     #break

# exit()

pool = ThreadPool()


pool = ThreadPool(12) # Sets the pool size to 12

# Open the urls in their own threads
# and return the results
results = pool.map(getfeed, urls)

#close the pool and wait for the work to finish 
pool.close()
pool.join()