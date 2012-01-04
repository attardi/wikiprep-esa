#!/usr/bin/python

'''
Copyright (C) 2010  Cagatay Calli <ccalli@gmail.com>

Adds anchors from Wikiprep output to target Wikipedia articles.

Legacy input format: <Target page id>  <Source page id>  <Anchor text (up to the end of the line)>
Input format: <Target page id>  <Source page id>  <Anchor location within text>  <Anchor text (up to the end of the line)>
Output format: <Target page id>    <Anchor text>

USAGE: addAnchors.py <anchor file from Wikiprep> <any writeable folder>
The folder is used by the script to create data files that are loaded into database.

IMPORTANT: If you use XML output from a recent version of Wikiprep
(e.g. Zemanta fork), then set FORMAT to 'Zemanta-legacy' or 'Zemanta-modern'.
'''

import os
import sys
import MySQLdb
from optparse import OptionParser
from subprocess import Popen, PIPE

# Wikiprep dump format enum
# formats: 1) Gabrilovich 2) Zemanta-legacy 3) Zemanta-modern
F_GABRI = 0 # gabrilovich
F_ZLEGACY = 1   # zemanta legacy
F_ZMODERN = 2   # zemanta modern

usage = """
USAGE: addAnchors.py <anchor files from Wikiprep> <any writeable folder>' --format=<Wikiprep dump format>
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

PARTITION_SIZE = 100000

if FORMAT == F_GABRI:
    FIELD_POS = 2
else:
    FIELD_POS = 3

outPrefix = os.path.join(args[-1], 'zanchor')
out = open(outPrefix + '0', 'w')
lc = 0
outk = 0

# usage python addAnchors.py enwiki-latest-pages-articles.anchor_text.000* out_dir --format=zm
for fname in args[:-1]:
    print >>sys.stderr, "  -> Processing file", fname
    #f = Popen(['zcat', fname], stdout=PIPE) # much faster than python gzip
    fpopen = Popen(['pigz', '-d', '-c', fname], stdout=PIPE) # even faster
    f = fpopen.stdout
    for i in range(3):
        f.readline() # skip header?
    for line in f:
        fields = line.split('\t')
        anc = fields[FIELD_POS].strip()
        if not anc:
            continue
        name = fields[0].strip()
        if not name:
            continue

        out.write("%s\t%s\n" % (name, anc))
        lc += 1
        if lc >= PARTITION_SIZE:
            lc = 0
            outk += 1
            out.close()
            out = open(outPrefix + str(outk), 'w')

out.close()
outk += 1

try:
        conn = MySQLdb.connect(host='localhost', user='root', passwd='123456', db='wiki', charset="utf8", use_unicode=True)
except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)

try:
    cursor = conn.cursor()
    for i in range(outk):
        si = str(i)
        cursor.execute("DROP TABLE IF EXISTS zanchor" + si)
        cursor.execute("CREATE TABLE zanchor" + si + " (target_id int(10) unsigned, anchor blob)")
        cursor.execute("LOAD DATA LOCAL INFILE '" + outPrefix + si + "' INTO TABLE zanchor" + si)
        cursor.execute("CREATE INDEX idx_target_id ON zanchor" + si + " (target_id);")

        cursor.execute("DROP TABLE IF EXISTS anchorList" + si)
        cursor.execute("CREATE TABLE anchorList" + si + " (target_id int(10) unsigned, anchor_text mediumblob)")
        cursor.execute("INSERT anchorList" + si + " SELECT a.target_id,GROUP_CONCAT(a.anchor SEPARATOR ' \n ') AS anchor_text FROM zanchor" + si + " a WHERE a.anchor IS NOT NULL GROUP BY a.target_id")

        cursor.execute("DROP TABLE zanchor" + si)

        # add anchors after creating each partition
        cursor.execute("CREATE INDEX idx_target_id ON anchorList" + si + " (target_id);")
        cursor.execute("UPDATE text t, anchorList" + si + " a SET t.old_text = CONCAT(a.anchor_text,' \n',t.old_text) WHERE t.old_id = a.target_id AND a.anchor_text IS NOT NULL;")
        cursor.execute("DROP TABLE anchorList" + si)

    cursor.close()
    conn.close()
except MySQLdb.Error, e:
    print "Error: %s" % e
    sys.exit(1)
