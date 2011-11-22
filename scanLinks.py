#!/usr/bin/python

'''
Copyright (C) 2011 Wavii <norman@wavii.com>

Scans XML output (gum.xml) from Wikiprep, in/out links count

USAGE: scanLinks.py file1.gz file2.gz ... > links.txt # file from Wikiprep
'''

import sys
import re
import collections
from subprocess import Popen, PIPE
import xmlwikiprep

MIN_LINKS_COUNT = 5

reOtherNamespace = re.compile("^(User|Wikipedia|File|MediaWiki|Template|Help|Category|Portal|Book|Talk|Special|Media|WP|User talk|Wikipedia talk|File talk|MediaWiki talk|Template talk|Help talk|Category talk|Portal talk):.+", re.DOTALL)

# pageContent - <page>..content..</page>
# pageDict - stores page attribute dict
def recordArticle(pageDoc, outgoing):
    # a simple check for content
    if pageDoc['length'] < 10:
        return

   # only keep articles of Main namespace
    if reOtherNamespace.match(pageDoc['title']):
        return

    _id = pageDoc['_id']
    outgoing[_id] = list(set(pageDoc['links'])) # go through set to remove dups
    
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "scanLinks.py file1.gz file2.gz ... > links.txt"
        sys.exit(1)

    print >>sys.stderr, "Creating outgoing list.."
    outgoing = {}

    for fname in sys.argv:
        print >>sys.stderr, "  -> Processing file", fname
        #f = Popen(['zcat', fname], stdout=PIPE) # much faster than python gzip
        f = Popen(['pigz', '-d', '-c', fname], stdout=PIPE) # even faster

        for doc in xmlwikiprep.read(f.stdout, set(['text'])):
            recordArticle(doc, outgoing)

    print >>sys.stderr, "Now creating incoming links count.."
    incoming_count = collections.defaultdict(int)
    for _id, links in outgoing.iteritems():
        for l in links:
            incoming_count[l] += 1

    print >>sys.stderr, "Output counts (id, outgoing, incoming).."
    for _id, in_count in incoming_count.iteritems():
        out_count = len(outgoing[_id])
        if out_count < MIN_LINKS_COUNT or in_count < MIN_LINKS_COUNT:
            continue
        print >>sys.stdout, '%d\t%d\t%d' % (_id, out_count, in_count)


# if nsBuflen > 0:
#     cursor.executemany("""
#     INSERT INTO namespace (id)
#         VALUES (%s)
#         """, nsBuffer)

#     nsBuffer = []
#     nsBuflen = 0

# if linkBuflen > 0:
#     cursor.executemany("""
#     INSERT INTO pagelinks (source_id,target_id)
#         VALUES (%s,%s)
#         """, linkBuffer)

#     linkBuffer = []
#     linkBuflen = 0


# cursor.execute("DROP TABLE IF EXISTS tmppagelinks")
# cursor.execute("CREATE TABLE tmppagelinks LIKE pagelinks")
# cursor.execute("INSERT tmppagelinks SELECT p.* FROM pagelinks p WHERE EXISTS (SELECT * FROM namespace n WHERE p.target_id = n.id)")
# cursor.execute("DROP TABLE pagelinks")
# cursor.execute("RENAME TABLE tmppagelinks TO pagelinks")

# # inlinks
# cursor.execute("DROP TABLE IF EXISTS inlinks")
# cursor.execute("CREATE TABLE inlinks AS SELECT p.target_id, COUNT(p.source_id) AS inlink FROM pagelinks p GROUP BY p.target_id")
# cursor.execute("CREATE INDEX idx_target_id ON inlinks (target_id)")

# # outlinks
# cursor.execute("DROP TABLE IF EXISTS outlinks")
# cursor.execute("CREATE TABLE outlinks AS SELECT p.source_id, COUNT(p.target_id) AS outlink FROM pagelinks p GROUP BY p.source_id")
# cursor.execute("CREATE INDEX idx_source_id ON outlinks (source_id)")

# cursor.close()
# conn.close()
