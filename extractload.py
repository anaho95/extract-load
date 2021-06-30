# Extract and Load Python Program
# Database server: PostgreSQL
# Ana Ho



# import needed libraries
import petl as etl
import psycopg2 as p
from sqlalchemy import *

# declare connection properties within the data dictionary
dbCons = {'operations': 'dbname=operations user=postgres password=password host=127.0.0.1',
          'python': 'dbname=python user=postgres password=password host=127.0.0.1'}

# set connections and cursors
sourceCon = p.connect(dbCons['operations'])  # grab value by referencing key dictionary
targetCon = p.connect(dbCons['python'])
sourceCursor = sourceCon.cursor()
targetCursor = targetCon.cursor()

# extract the names of the source tables to be copied
sourceCursor.execute("""select table_name from information_schema.columns where table_name in ('salesperson',
'returns') group by 1""")
sourceTables = sourceCursor.fetchall()


# iterate through table names to load into python database
for t in sourceTables:
    targetCursor.execute("drop table if exists %s" % (t[0]))
    sourceDataset = etl.fromdb(sourceCon, 'select * from %s' % (t[0]))
    etl.todb(sourceDataset, targetCon, t[0], create=True, sample=10000)





