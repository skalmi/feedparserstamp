import feedparser
import json
import time
import calendar
import os
import urllib2
import sys
import csv

from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

csvReader = csv.reader(open('urls.csv', 'rb'));

feedparser.USER_AGENT = "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)"

debuglevel=0
h = urllib2.HTTPHandler(debuglevel)

def rss2file(href):
    path_name = time.strftime('%Y%m%d')
    if not os.path.exists(path_name):
        os.makedirs(path_name)

    try:

        opener = urllib2.build_opener(h)
        opener.addheaders = [('User-Agent', feedparser.USER_AGENT)]
        response = opener.open(href)
        html = response.read()
        d = feedparser.parse(html)

    except:
        #print "Unexpected error:", sys.exc_info()[0]
        return 0

    e = enumerate(d.entries)
    for y, element in e:
        if "published_parsed" in element:
            if element.published_parsed:
                d['entries'][y]['published_parsed'] = calendar.timegm(element.published_parsed)
        if "updated_parsed" in element:
            if element.updated_parsed:
                d['entries'][y]['updated_parsed'] = calendar.timegm(element.updated_parsed)
                #del d['entries'][y]['updated_parsed']
    json_string = json.dumps(d.entries)
    href_name = href.replace('&', '#_#')
    href_name = href_name.replace('://', '__')
    href_name = href_name.replace('/', '_')

    my_file_json = open(path_name + os.sep + href_name + '=-=' + time.strftime('%H%M%S') + '.json', 'wb+')
    my_file_json.write(json_string)
    my_file_json.close()

    my_file_xml = open(path_name + os.sep + href_name + '=-=' + time.strftime('%H%M%S') + '.xml', 'wb+')
    my_file_xml.write(html)
    my_file_xml.close()

    #print '= OK'
    return 1

# for row in csvReader:
#     rss2file(row[0])


urls = [
    'http://feeds.feedburner.com/Hullabaloo',
    'http://politicalstar.com/feed/',
    'http://wspus.org/feed/',
    'http://majoritywhip.gov/rss.xml',
    'http://hrw.org/rss/taxonomy/137',
    'http://rss.nytimes.com/services/xml/rss/nyt/World.xml',
    'http://telegraph.co.uk/rss',
    'http://feeds.feedburner.com/juancole/ymbn',
    'http://politicalmemes.com/feed/',
    'http://feeds.feedburner.com/TaxPolicyBlog'
]


pool = ThreadPool()


pool = ThreadPool(4) # Sets the pool size to 4

# Open the urls in their own threads
# and return the results
results = pool.map(rss2file, urls)

#close the pool and wait for the work to finish 
pool.close()
pool.join()