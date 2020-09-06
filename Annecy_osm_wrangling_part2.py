#!/usr/bin/env python
# coding: utf-8

# 
# # Project 2: Wrangle OpenStreetMap Data
# 
# =============================================================================
#                  PART 2 from CSV files to SQL Data basis and queries
# =============================================================================
# 

"""
Area explored is that of Annecy, in the French Alps, a place that I like and where part of my family leaves.
Annecy is near French Alps, not far from Switzerland.
minlat="45.7996000" minlon="5.9336000" maxlat="45.9323000" maxlon="6.2529000"
In  py file Annecy_osm_wrangling_part1.py, dataset was corrected and five
csv files created :
#
nodes.csv (nodes attributes ie id, lat, lon, user, uid, version, changeset and timestamp ;
nodes_tags.csv (id, key, value and type)
ways.csv = [id, user, uid,version, changeset and timestamp]
ways_nodes.csv = [id, key, value, type]
ways.tags.csv [id, node_id, position]
# 
"""

# 
# Import necessary python modules for the analysis and define path to dataset
# 
# 
# 

# In[1]:


#!/usr/bin/env python
# coding: utf-8import xml.etree.cElementTree as ET 

import codecs ##to write unicode files
import pprint ## to print easier to read dictionnaries
import os ## to get file size 
import pandas as pd ## I am more familiar with dataframe use than plan dictionnaries
import re ## to search characters in dataset
import sqlite3 ## to use SQL databasis
from collections import defaultdict ## to avoid Keyerrors. Will return default value for missing values. 
                                    ##As advised by Udacity:-)
DATASET = "Annecy.osm"

pd.set_option('display.width', 2000) ## in order to get nicer display of pandas dataframes on screen
pd.set_option('max_colwidth', -1)


# <a id='assess'></a>
#    ## Assess data

#  ### Check that size of the file is greater than 50Mb#




# =============================================================================
# CREATE SQL DATABASE
# =============================================================================

# In[14]:




# Creating database on disk
sqlite_file = 'Annecy.db'
conn = sqlite3.connect('Annecy.db')
c = conn.cursor()

c.execute('''DROP TABLE IF EXISTS nodes''')
c.execute('''DROP TABLE IF EXISTS nodes_tags''')
c.execute('''DROP TABLE IF EXISTS ways''')
c.execute('''DROP TABLE IF EXISTS ways_tags''')
c.execute('''DROP TABLE IF EXISTS ways_nodes''')
conn.commit()


# **Create sql tables**

# In[15]:


QUERY_NODES = """


CREATE TABLE nodes (
    id INTEGER NOT NULL,
    lat REAL,
    lon REAL,
    user TEXT,
    uid INTEGER,
    version INTEGER,
    changeset INTEGER,
    timestamp TEXT
);
"""

QUERY_NODES_TAGS = """
CREATE TABLE nodes_tags (
    id INTEGER,
    key TEXT,
    value TEXT,
    type TEXT,
    FOREIGN KEY (id) REFERENCES nodes(id)
);
"""

QUERY_WAYS = """
CREATE TABLE ways (
    id INTEGER NOT NULL,
    user TEXT,
    uid INTEGER,
    version INTEGER,
    changeset INTEGER,
    timestamp TEXT
);
"""

QUERY_WAYS_TAGS = """
CREATE TABLE ways_tags (
    id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    type TEXT,
    FOREIGN KEY (id) REFERENCES ways(id)
);
"""

QUERY_WAYS_NODES = """
CREATE TABLE ways_nodes (
    id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    FOREIGN KEY (id) REFERENCES ways(id),
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);
"""



c.execute(QUERY_NODES)
c.execute(QUERY_NODES_TAGS)
c.execute(QUERY_WAYS)
c.execute(QUERY_WAYS_TAGS)
c.execute(QUERY_WAYS_NODES)

conn.commit()


# In[16]:


with open('nodes.csv','rt',encoding='utf8') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db1 = [(i['id'], i['lat'], i['lon'], i['user'], i['uid'], i['version'], i['changeset'], i['timestamp']) for i in dr]
    
with open('nodes_tags.csv','rt',encoding='utf8') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db2 = [(i['id'], i['key'], i['value'], i['type']) for i in dr]
    
with open('ways.csv','rt',encoding='utf8') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db3 = [(i['id'], i['user'], i['uid'], i['version'], i['changeset'], i['timestamp']) for i in dr]
    
with open('ways_tags.csv','rt',encoding='utf8') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db4 = [(i['id'], i['key'], i['value'], i['type']) for i in dr]
    
with open('ways_nodes.csv','rt',encoding='utf8') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db5 = [(i['id'], i['node_id'], i['position']) for i in dr]


# In[17]:


c.executemany("INSERT INTO nodes(id, lat, lon, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?);", to_db1)
c.executemany("INSERT INTO nodes_tags(id, key, value, type) VALUES (?, ?, ?, ?);", to_db2)
c.executemany("INSERT INTO ways(id, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?);", to_db3)
c.executemany("INSERT INTO ways_tags(id, key, value, type) VALUES (?, ?, ?, ?);", to_db4)
c.executemany("INSERT INTO ways_nodes(id, node_id, position) VALUES (?, ?, ?);", to_db5)
conn.commit()


# <a id='checkdb'></a>
#    ## Some checks on databasis accuracy and completeness prior to inquiries

# Several preliminary checks on SQL databasis
# - Check that databasis is complete (same number of nodes and ways as computed before)
# - Check that some wrong street names have been corrected

# In[18]:


c.execute('SELECT COUNT(*) FROM nodes')
all_rows = c.fetchall()
print(all_rows)

c.execute('SELECT COUNT(*) FROM ways')
all_rows = c.fetchall()
print(all_rows)


# In[19]:


CHECK_CHANGES2= """
SELECT value 
FROM ways_tags
WHERE value ='Chemin de Bellevue'
OR value ='Rue Cassiopée'
OR value ='rue Cassiopée'
OR value ='Chemin de Bellevue'
limit 10;
"""
c.execute(CHECK_CHANGES2)
all_rows=c.fetchall()
print(all_rows)


# In[20]:


CHECK_CHANGES3= """
SELECT value 
FROM nodes_tags
WHERE value ='Chemin de Bellevue'
OR value ='Rue Cassiopée'
OR value ='rue Cassiopée'

;
"""
c.execute(CHECK_CHANGES3)
all_rows=c.fetchall()
print(all_rows)


#

# _Enquiries about content of the database in SQL, at least..._

# **Count number of most active users**
# 

# In[21]:


# Query to show the nicknames *user* and contributions of the top 15 contributors
QUERY = '''
SELECT DISTINCT nodes.user as USER, COUNT(*) as contribution_number
FROM nodes
GROUP BY nodes.user
ORDER BY COUNT(*) DESC
LIMIT 15;
'''
c.execute(QUERY)
all_rows = c.fetchall()
contributors=pd.DataFrame([all_rows]).T
contributors.columns=(["users, number of contributions"])
print(contributors)


# **How many contributors and their distribution in % of contributions**   
# We can see that Emmanuel Pacaud is the most active contributor, with a little more than 50% of contributions. 
# JFK 73 is second contributor with ten time less contributions however 31147 entries.
# We also see that total of distinct contributors is 793 (to be compared with 926 in the initial .osm file). This is however not surprising because we did not imprt all elements in sql (relations not included)
# I would have liked to draw distribution but I cannot spit information in a single list or column.
# 

# In[22]:


# Query to show and contributions allcontributors in %
QUERY = '''
SELECT DISTINCT nodes.user, COUNT(*) * 100.0 / (SELECT COUNT(*) FROM nodes)
FROM nodes
GROUP BY nodes.user
ORDER BY (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM nodes)) DESC
;
'''

c.execute(QUERY)
all_rows = c.fetchall()
contributors=pd.DataFrame([all_rows]).T
print(type(contributors))
##contributors.str.split(pat=",")
print(contributors)
##seaborn.distplot(contributors, kde=True);


# **What are main shops keys and number for each key, list first 40**

# In[23]:


LIST_NTSHOPS="""
select value,count(value)
from nodes_tags
WHERE key ='shop'
group by value
order by count(value) desc

;
"""
c.execute(LIST_NTSHOPS)
all_rows=c.fetchall()
z=pd.DataFrame([all_rows]).T
print(z.iloc[0:20,])


# In[24]:


print(z.iloc[20:40,])


# **What are other informations for nodes shops.yes**

# In[25]:


SHOPSYES="""
select nodes_tags.id, nodes_tags.key, nodes_tags.value from nodes_tags,
((select nodes.id as yesyes
from  nodes_tags left join nodes
ON nodes.id=nodes_tags.id
AND nodes_tags.key ='shop'
AND nodes_tags.value ='yes') as sousprog)

WHERE nodes_tags.id = yesyes

;
"""

c.execute(SHOPSYES)
all_rows=c.fetchall()
z=pd.DataFrame([all_rows]).T
print(z)


# **What are other informations for nodes shops.cheese ( five shops selling exclusively cheese, we are in French mountains)**

# In[26]:


SHOPSCHEESE="""
select nodes_tags.id, nodes_tags.key, nodes_tags.value from nodes_tags,
((select nodes.id as cheeses
from  nodes_tags left join nodes
ON nodes.id=nodes_tags.id
AND nodes_tags.key ='shop'
AND nodes_tags.value ='cheese') as sousprog)

WHERE nodes_tags.id = cheeses

;
"""

c.execute(SHOPSCHEESE)
all_rows=c.fetchall()
z=pd.DataFrame([all_rows]).T
print(z)


# **What are other informations for nodes shops.deli**  
# _( Shop focused on selling delicatessen , possibly also fine wine. Not to be confused with the US delis.)_

# In[27]:


SHOPSDELI="""
select nodes_tags.id, nodes_tags.key, nodes_tags.value from nodes_tags,
((select nodes.id as delis
from  nodes_tags left join nodes
ON nodes.id=nodes_tags.id
AND nodes_tags.key ='shop'
AND nodes_tags.value ='deli') as sousprog)

WHERE nodes_tags.id = delis
order by nodes_tags.id
;
"""

c.execute(SHOPSDELI)
all_rows=c.fetchall()
z=pd.DataFrame([all_rows]).T
print(z)


# **What are the main amenities, list first 50 and count for each key**

# In[28]:


LIST_NTAMEN="""
select value,count(value)
from nodes_tags
WHERE key ='amenity'
group by value
order by count(value) desc

;
"""
c.execute(LIST_NTAMEN)
all_rows=c.fetchall()
z=pd.DataFrame([all_rows]).T
print(z.iloc[0:50,])


# **What are main "historic" keys ?**

# In[29]:


LIST_HISTORIC="""
select value,count(value)
from nodes_tags
WHERE key ='historic'
group by value
order by count(value) desc

;
"""

c.execute(LIST_HISTORIC)
all_rows=c.fetchall()
z=pd.DataFrame([all_rows]).T
print(z)


# **There are 5 archeological sites, what are other informations (nodes_tags) about those archeological sites**

# In[30]:


ARCH_HISTORIC="""
select nodes_tags.id, nodes_tags.key, nodes_tags.value from nodes_tags,
((select nodes.id as sitesarcheo
from  nodes_tags left join nodes
ON nodes.id=nodes_tags.id
AND nodes_tags.key ='historic'
AND nodes_tags.value ='archaeological_site') as sousprog)

WHERE nodes_tags.id = sitesarcheo
order by nodes_tags.id
;
"""

c.execute(ARCH_HISTORIC)
all_rows=c.fetchall()
z=pd.DataFrame([all_rows]).T
print(z)


# <a id='references'></a>
#    ## Some links to references used

# **General**  
# - Map feature in openstreetmap https://wiki.openstreetmap.org/wiki/Map_Features (list of nodes and tags to use)
# - Second main user of OSM in Annecy area has hit headlines (:-)https://www.salzburgresearch.at/blog/alpine-ski-world-championship-meets-openstreetmap-who-are-the-mapping-champions/
# 
# 
# 
# **Solutions to some python coding problems**  
# - VERBOSE and Re Module https://www.geeksforgeeks.org/verbose-in-python-regex/
# - csv write and headers https://discuss.codecademy.com/t/how-does-writeheader-know-the-fields-to-use/463772  
# - how to get nice display of pandas dataframe (correct width) https://stackoverflow.com/questions/11707586/how-do-i-expand-the-output-display-to-see-more-columns-of-a-pandas-dataframe
# - tool for count of nodes, ways and relations https://gis.stackexchange.com/questions/281788/openstreetmap-determining-number-of-nodes-ways-and-relations-in-a-pbf-file
# - fo export from osm file to csv file : https://mygeodata.cloud/converter/osm-to-csv#:~:text=Upload%20your%20OSM%20data%20(widely,will%20be%20exported%20as%20well.
# 
# **Modify database in sql**  
# https://www.w3schools.com/sql/sql_update.asp
# 
# **Various Github references for OSM Wrangling (not always fully trustworthy but good ideas for project debugging)**
# https://github.com/wblakecannon/udacity-dand/tree/master/4-Data-Wrangling  
# https://hadrien-lcrx.github.io/notebooks/Boston_Data_Wrangling.html  
# http://napitupulu-jon.appspot.com/posts/wrangling-openstreetmap.html  
# https://github.com/GloSanc/Wrangle_OSM
# 

# In[ ]:




