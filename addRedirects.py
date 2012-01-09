#!/usr/bin/python

'''
Copyright (C) 2010  Cagatay Calli <ccalli@gmail.com>

Adds redirections from Wikiprep output to target Wikipedia articles.

USAGE: addRedirects.py <redir file from Wikiprep> <any writeable folder>
The folder is used by the script to create data files that are loaded into database.

IMPORTANT: If you use XML output from a recent version of Wikiprep
(e.g. Zemanta fork), then set FORMAT to 'Zemanta-legacy' or 'Zemanta-modern'.
'''

import os
import re
import sys
import MySQLdb
from optparse import OptionParser

# Wikiprep dump format enum
# formats: 1) Gabrilovich 2) Zemanta-legacy 3) Zemanta-modern
F_GABRI = 0 # gabrilovich
F_ZLEGACY = 1   # zemanta legacy
F_ZMODERN = 2   # zemanta modern

usage = """
USAGE: addRedirects.py <redir.xml file from Wikiprep> <any writeable folder>' --format=<Wikiprep dump format>
Wikiprep dump formats:
1. Gabrilovich [gl, gabrilovich]
2. Zemanta legacy [zl, legacy, zemanta-legacy]
3. Zemanta modern [zm, modern, zemanta-modern]
"""

parser = OptionParser(usage=usage)
parser.add_option("-f", "--format", dest="_format", help="Wikiprep dump format (g for Gabrilovich, zl for Zemanta-legacy,zm for Zemanta-modern)", metavar="FORMAT")
(options, args) = parser.parse_args()
if len(args) < 2:
    print usage
    sys.exit()
if not options._format:
    print """
Wikiprep dump format not specified! Please select one from below with --format option:

Wikiprep dump formats:
1. Gabrilovich [gl, gabrilovich]
2. Zemanta legacy [zl, legacy, zemanta-legacy]
3. Zemanta modern [zm, modern, zemanta-modern]
"""
    sys.exit()

if options._format in ['zm', 'zemanta-modern', 'Zemanta-modern', 'Zemanta-Modern', 'modern']:
        FORMAT = F_ZMODERN
elif options._format in ['gl', 'gabrilovich', 'Gabrilovich']:
        FORMAT = F_GABRI
elif options._format in ['zl', 'zemanta-legacy', 'Zemanta-legacy', 'Zemanta-Legacy', 'legacy']:
        FORMAT = F_ZLEGACY

PARTITION_SIZE = 2000000
RSIZE = 10000000        # read chunk size = 10 MB - implicit for now

reModernREDIR = re.compile('<redirect>\n<from>\n<id>.+?</id>\n<name>(?P<text>.+?)</name>\n</from>\n<to>\n<id>(?P<target>\d+)</id>\n<name>.+?</name>\n</to>\n</redirect>', re.DOTALL | re.MULTILINE)
reLegacyREDIR = re.compile('<redirect>\n<from>\n<id>.+?</id>\n<title>(?P<text>.+?)</title>.*?</from>\n<to>\n<id>(?P<target>\d+)</id>\n<title>.+?</title>\n</to>\n</redirect>', re.DOTALL | re.MULTILINE)

if FORMAT == F_ZMODERN:
    reREDIR = reModernREDIR
else:
    reREDIR = reLegacyREDIR

try:
    conn = MySQLdb.connect(host='localhost', user='root', passwd='123456', db='wiki', charset="utf8", use_unicode=True)
except MySQLdb.Error, e:
    print "Error %d: %s" % (e.args[0], e.args[1])
    sys.exit(1)

outPrefix = os.path.join(args[-1], 'zredir')

lc = 0
outk = 0

for fname in args[:-1]:
    print >>sys.stderr, "  -> Processing file", fname
    f = open(fname)

    prevText = ''
    firstRead = f.read(10000)

    documentStart = firstRead.find('<redirects>') + len('<redirects>')
    prevText = firstRead[documentStart:10000]
    out = open(outPrefix + str(outk), 'w')

    while True:

        newText = f.read(RSIZE)
        if not newText:
            break

        text = prevText + newText
        endIndex = -1

        for page in reREDIR.finditer(text):
            out.write(page.group(2) + '\t' + page.group(1) + '\n')
            lc += 1

            if lc >= PARTITION_SIZE:
                lc = 0
                outk += 1
                out.close()
                out = open(outPrefix + str(outk), 'w')

            endIndex = page.end()

        prevText = text[endIndex:]

if lc > 0:
    out.close()

outk += 1

try:
    cursor = conn.cursor()

    for i in range(outk):
        si = str(i)
        cursor.execute("DROP TABLE IF EXISTS zredir" + si)
        cursor.execute("CREATE TABLE zredir" + si + " (target_id int(10) unsigned, redir varbinary(255));")
        cursor.execute("LOAD DATA LOCAL INFILE '" + outPrefix + si + "' INTO TABLE zredir" + si)
        cursor.execute("CREATE INDEX idx_target_id ON zredir" + si + " (target_id);")

        cursor.execute("DROP TABLE IF EXISTS redirList" + si)
        cursor.execute("CREATE TABLE redirList" + si + " SELECT a.target_id,GROUP_CONCAT(a.redir SEPARATOR ' \n') AS redir_text FROM zredir" + si + " a WHERE a.redir IS NOT NULL GROUP BY a.target_id;")

        cursor.execute("DROP TABLE zredir" + si)

        # add redirects after creating each partition
        cursor.execute("CREATE INDEX idx_target_id ON redirList" + si + " (target_id);")
        cursor.execute("UPDATE text t, redirList" + si + " a SET t.old_text = CONCAT(a.redir_text,' \n',t.old_text) WHERE t.old_id = a.target_id AND a.redir_text IS NOT NULL;")
        cursor.execute("DROP TABLE redirList" + si)

    cursor.close()
    conn.close()
except MySQLdb.Error, e:
    print "Error: %s" % e
    sys.exit(1)
