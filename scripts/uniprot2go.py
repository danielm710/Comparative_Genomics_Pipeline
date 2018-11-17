#!/usr/bin/env python
"""
This is a slightly modified version of Doxey Lab's uniprot2go.py
"""

import pysqlw
import sys, getopt

sqlfile = ''
uniprotlist = ''

try:
   opts, args = getopt.getopt(sys.argv[1:],"hi:d:",["uniprotlist=","sqlfile="])
except getopt.GetoptError:
   print 'uniprot2go.py -i <uniprotlist.txt> -d <sql.db>'
   sys.exit(2)
if len(sys.argv) <= 1:
    print('uniprot2go.py -i <uniprotlist.txt> -d <sql.db>')
    sys.exit()
for opt, arg in opts:
   if opt == '-h':
      print 'uniprot2go.py -i <uniprotlist.txt> -d <sql.db>'
      sys.exit()
   elif opt in ("-i", "--uniprotlist"):
      uniprotlist = arg
   elif opt in ("-d", "--sqlfile"):
      sqlfile = arg


#print 'DB =  ', sqlfile
#print 'UNIPROTLIST = ', uniprotlist


p = pysqlw.pysqlw(db_type="sqlite", db_path=sqlfile)

filepath = uniprotlist

#assumes no header
with open(filepath, 'r') as fp:
    for line in fp:
        line = line.strip()
        uni = line
        #print(line)
        #print(uni)
        rows = p.where('uniprotid',uni).get('unitogo')
        goTerms = []
        for b in rows:
            #print(b['goTerm']);
            goTerms.append(b['goTerm'])
        goTerms = set(goTerms)
        #goTerms.reverse()
        print(line + "\t" + ','.join(goTerms))

p.close();
