#!/usr/bin/env python
# coding: utf-8

# 
# # Project 2: Wrangle OpenStreetMap Data
# 
#  ## Project : cleaning and sql queries of Annecy open street map data
# 
# ![https://github.com/MariannePERAUD/Udacity_Project_2_Data_wrangling_with_sql/blob/master/Annecy.jpg](Annecy.jpg)
# =======================================================#
#               TABLE OF CONTENTS                        #
# ====================================================== #

# Introduction
# Initial Set-up
# Assess data
# Identify problems and clean data
# Write nodes and ways as csv files
# 

# 
# =======================================================#
#               INTRODUCTION                             #
# ====================================================== #
# 
# Area explored is that of Annecy, in the French Alps, a place that I like and where part of my family leaves.
# Annecy is near French Alps, not far from Switzerland.
# 
# 
# 
#  minlat="45.7996000" minlon="5.9336000" maxlat="45.9323000" maxlon="6.2529000"
# 
# I will first check file size and content ; 
# 
# Then identify problems and errors and begin data cleaning to make further analysis on a cleaned dataset;  
# 
# Transform osm data in csv.files ;  
# 
# 
# 
# 
# 
# 

# 
# ## Set-up
# Import necessary python modules for the analysis and define path to dataset
# 
# 
# 

# In[1]:


#!/usr/bin/env python
# coding: utf-8import xml.etree.cElementTree as ET 
import xml.etree.cElementTree as ET 
import codecs ##to write unicode files
import csv ## to read and write csv file
import pprint ## to print easier to read dictionnaries
import os ## to get file size 
import pandas as pd ## I am more familiar with dataframe use than plan dictionnaries
import re ## to search characters in dataset
from collections import defaultdict ## to avoid Keyerrors. Will return default value for missing values. 
                                    ##As advised by Udacity:-)
DATASET = "Annecy.osm"

pd.set_option('display.width', 2000) ## in order to get nicer display of pandas dataframes on screen
pd.set_option('max_colwidth', -1)



#    ## Assess data

# =======================================================#
#               ASSESS DATA FUNCTIONS                    #
# ====================================================== #

# In[2]:

def TAILLE(filename) :
    """ 
    Aim is to check dataset size
  
    os.path.getsize is the os module way to get size from a file in bytes as well
    as lot of information about operating system.
    to get size in MB, we will divide value obtain by 1M and round it to integer part.
  
    Parameters: 
    filename : should contain name of data set, that has previously been named DATASET
    for easy work, osm file should be in the same path as this query, otherwise, path should be defined 
    at line 57 DATASET = ...
    
    Documentation used :
    Map's size and references
    Source: https://prograide.com/pregunta/6464/obtenir-la-taille-du-fichier-en-python
    Returns: 
    size : file size in Mb as integer value
    
    """
    size=os.path.getsize(filename)/1000000
    size=round(size)
    return size



#  ### Count number of different tags in the file#

# In[3]:




def COMPTE_TAGS(filename):
    """
    Aim is to make a list of tags used in osm file, with number of each distinct
    reference
    events are xml events
    search for information near open key, if it is a tag already encountered, adds 1 
    to count of this tag, else add a new line in dictionnary with tag and count 1
    
    
    Parameters
        filename should contain name of data set, that has previously been named DATASET
        for easy work, osm file should be in the same path as this query, otherwise, path should be defined 
        at line 57 DATASET = ...
    
    Returns tags, dictionnary with tags keys and number of occurences for each
    """ 
    tags={}
    for event, elem in ET.iterparse(filename, events=("start",)):
        if elem.tag in tags.keys():
            tags[elem.tag] += 1
        else:
            tags[elem.tag] = 1
    
    return tags



  
    ## see summary at the end of paragraph


#  ### Count Number of users as of August 28th 2020 (extraction date)#

# In[4]:





def UTILISATEURS(filename):
    """
    Aim is to count user ids in the filename.
    function creates a set, then records in this set all occurences of tags with['uid']
    attribute.
    
    Parameters
    filename should contain name of data set, that has previously been named DATASET
    for easy work, osm file should be in the same path as this query, otherwise, path should be defined 
    at line 57 DATASET = ...
    
    """
    users = set()
    for Marianne, element in ET.iterparse(filename):
        try:
            users.add(element.attrib['uid'])
        except KeyError:
            continue

    return users



    ##see summary at the end of paragraph


# In[ ]:





#  ### Statistics summary

# In[5]:
# ============================================================== #
#               ASSESS DATA : OUTPUT FUNCTION                    #
# ============================================================== #

"""
All previous function executed for DATASET and presented so as to be copied in
final report
"""
size =TAILLE(DATASET)
tags = COMPTE_TAGS(DATASET)
users = UTILISATEURS(DATASET)
print(DATASET,"file : MAIN INFORMATION")
print("")                                                             
print("Size of Annecy.osm file is",size,"Mb, so higher than 50Mb requested")
print("Number of tags per type")
print(pd.DataFrame([tags]).T)
print("")
print(len(users),"users until August 28th 2020")


# ============================================================== #
#               IDENTIFY PROBLEMS and CLEAN DATA                 #
# ============================================================== #
# 
# Analyses focuses on identifying problems in street names
# 

# ### Assess various types of street names
"""
French street names are beginning with streetname type,  
Rue Lafayette or Allées Wilson etc...  
In another node, you can find housenumber  
Decoding of Annecy street names has to be a little different from that of US street names.

street_type_re is what will be identified as street type in elements
expected is a list of expected street types in a French osm file (Routes, Rues...)
"""

# In[6]:



street_type_re = re.compile(r"""
^                                    ## for the beginning of the string
\b                                   ## might be empty, but only at the beginning
\S+                                  ## then followed by any character that is not a space
\.?""",                              
re.IGNORECASE|re.VERBOSE) 



expected = ["Routes","Maison","ZA","ZI","Palais","Parc","Angon","Escaliers","Le","Les","Lieudit","Chemin","Esplanade","Faubourg","Passage","Pont","Port","Rue", "Boulevard","Place","Allée","Avenue","Impasse","Col","Côte","Quai","Rampe","Route","Promenade","Square","Voie"]
##when iterating on the query, we will see that there are many valid possible entries other than rue (=street)




# Nested functions in order to audit open street map file

# In[7]:




def audit_street_type(street_types, street_name):
    """
    aim is to search all information of street type (called m) and create a dictionnary
    grouped by street type and called street_types
    
    Parameters : street_types : set
                 street_name  : dictionnary
    Return : street_types 
    """
    m = street_type_re.search(street_name)
   
    if m:
        street_type = m.group()
        if street_type not in expected:
           
            street_types[street_type].add(street_name)

 
def is_street_name(elem):
    """
    defines condition : is in street types
    
    parameter elem : string
    returns  True or FALSE
    """
    return (elem.attrib['k'] == "addr:street")


def audit(osmfile):
    """
    Aim is to parse DATASET, 
    First create a empty defaultdict set called street_types,
    then parses osm file tags, if node or way, and if tag is a street name
    (sub fuction defined above)
    performs function audit_street_type defined above
    
    returns a collection.defaultdictionnary with all types of street names and their occurences.
    

    """
    osm_file = open(DATASET, "r",encoding='utf-8')
    street_types = defaultdict(set)
  
    i=0
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        
        if elem.tag == "node" or elem.tag == "way":
           
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])

    return street_types


# In[8]:


##run funtion audit on osm file and print "unexpected street type dictionnary"


st_types = audit(DATASET)

pprint.pprint(dict(st_types))


# New mapping dictionary 13 and Georges are not changed at this stage  
#                          13 should go in another tag (housenumber) and name of the street type should be added to  
#                          street_nametag at a later stage; 
# Lower case is replaced and abbreviation fg is replaced by Faubourg.
# 
# If users are not paying attention to street names, can we consider dataset as reliable ?
# It depends on what we are looking for.
# Where I leave, I use open street map for recommended itineraries by bicycle, so street names are not so important for me.
# What is more important is what I can find on my way (shops, parks...).
# 
# However, let's replace wrong entries in the file that will be imported for queries.
# 

# **Replace wrong upercase/lower case**

# In[9]:



"""
mapping is a dictionnary of strings to be corrected
"""
mapping = { "13": "13",
           "Georges": "Georges",
            "allée.": "Allée",
            "chemin": "Chemin",
            "fg": "Faubourg",
            "route": "Route",
          "rue": "Rue"}

#creates a new dictionnary with corrected street type, when necessary
def update_name(name):
    """
    Aim : replacing in street names, street type by corrected street type
    Parameter :name string
    returns street_type : first string of caracters before blank
    returns street_name : last string of caracters unto blank.
    returns name = street type + blank + street_name
    """
    street_type= name.split(' ',1)[0]
    street_name= name.split(' ',1)[-1]        

    if street_type in mapping:
        name = mapping[street_type] + ' ' + street_name  

    return name




# In[10]:


#run update creates a new dictionnary where street type entries are corrected when necessary
def run_updates(filename):
    
    for st_type, ways in st_types.items():
        for name in ways:
            better_name = update_name(name)
            if better_name != name:
                corrected_names[name] = better_name
    return corrected_names


# In[11]:


#corrected names creation for osm file
corrected_names = {}   
corrected_names = run_updates(DATASET)


check2=(pd.DataFrame([corrected_names]).T) 
print("**QUICK CHECK OF MODIFICATIONS PROPOSAL**",check2)


# 
#    ## Write nodes and ways as csv files

# File is now ready for import in a csv file and then in SQL databasis.
# We will create five csv files preparing five SQL tables creation
# 

# In[12]:

# =============================================================================
#                             CREATION OF CSV FILES
# =============================================================================


NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"



# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

# Prepare modification of dictionnary to import with modified street names
def correct_element(v):
    if v in corrected_names:
        correct_value = corrected_names[v]
    else:
        correct_value = v
    return correct_value

def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                   default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    if element.tag == 'node':
        node_attribs['id'] = element.attrib['id']
        node_attribs['user'] = element.attrib['user']
        node_attribs['uid'] = element.attrib['uid']
        node_attribs['version'] = element.attrib['version']
        node_attribs['lat'] = element.attrib['lat']
        node_attribs['lon'] = element.attrib['lon']
        node_attribs['timestamp'] = element.attrib['timestamp']
        node_attribs['changeset'] = element.attrib['changeset']
        
        for node in element:
            tag_dict = {}
            tag_dict['id'] = element.attrib['id']
            if ':' in node.attrib['k']:
                tag_dict['type'] = node.attrib['k'].split(':', 1)[0]
                tag_dict['key'] = node.attrib['k'].split(':', 1)[-1]
                tag_dict['value'] = correct_element(node.attrib['v'])
            else:
                tag_dict['type'] = 'regular'
                tag_dict['key'] = node.attrib['k']
                tag_dict['value'] = correct_element(node.attrib['v'])
            tags.append(tag_dict)
            
    elif element.tag == 'way':
        way_attribs['id'] = element.attrib['id']
        way_attribs['user'] = element.attrib['user']
        way_attribs['uid'] = element.attrib['uid']
        way_attribs['version'] = element.attrib['version']
        way_attribs['timestamp'] = element.attrib['timestamp']
        way_attribs['changeset'] = element.attrib['changeset']
        n = 0
        for node in element:
            if node.tag == 'nd':
                way_dict = {}
                way_dict['id'] = element.attrib['id']
                way_dict['node_id'] = node.attrib['ref']
                way_dict['position'] = n
                n += 1
                way_nodes.append(way_dict)
            if node.tag == 'tag':
                tag_dict = {}
                tag_dict['id'] = element.attrib['id']
                if ':' in node.attrib['k']:
                    tag_dict['type'] = node.attrib['k'].split(':', 1)[0]
                    tag_dict['key'] = node.attrib['k'].split(':', 1)[-1]
                    tag_dict['value'] = correct_element(node.attrib['v'])
                else:
                    tag_dict['type'] = 'regular'
                    tag_dict['key'] = node.attrib['k']
                    tag_dict['value'] = correct_element(node.attrib['v'])
                tags.append(tag_dict)
    
    if element.tag == 'node':
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()



class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, str) else v) for k, v in row.items()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# In[13]:


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in):
    """Iteratively process each XML element and write to csv(s)"""

    ## create csv files
    with codecs.open(NODES_PATH, 'w',encoding='utf-8') as nodes_file,     codecs.open(NODE_TAGS_PATH, 'w',encoding='utf-8') as nodes_tags_file,     codecs.open(WAYS_PATH, 'w',encoding='utf-8') as ways_file,     codecs.open(WAY_NODES_PATH, 'w',encoding='utf-8') as way_nodes_file,     codecs.open(WAY_TAGS_PATH, 'w',encoding='utf-8') as way_tags_file:

        nodes_writer = csv.DictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = csv.DictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = csv.DictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = csv.DictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = csv.DictWriter(way_tags_file, WAY_TAGS_FIELDS)

    ## create headers in csv files
        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if element.tag == 'node':
                nodes_writer.writerow(el['node'])
                node_tags_writer.writerows(el['node_tags'])
            elif element.tag == 'way':
                ways_writer.writerow((el['way']))
                way_nodes_writer.writerows(el['way_nodes'])
                way_tags_writer.writerows(el['way_tags'])
                    
process_map(DATASET)



# In[30]:
# 
#    ## Some links to references used
#
# **General**  
# - Map feature in openstreetmap https://wiki.openstreetmap.org/wiki/Map_Features (list of nodes and tags to use)
#
# 
# **Solutions to some python coding problems**  
# - VERBOSE and Re Module https://www.geeksforgeeks.org/verbose-in-python-regex/
# - csv write and headers https://discuss.codecademy.com/t/how-does-writeheader-know-the-fields-to-use/463772  
# - how to get nice display of pandas dataframe (correct width) https://stackoverflow.com/questions/11707586/how-do-i-expand-the-output-display-to-see-more-columns-of-a-pandas-dataframe
# - tool for count of nodes, ways and relations https://gis.stackexchange.com/questions/281788/openstreetmap-determining-number-of-nodes-ways-and-relations-in-a-pbf-file
# - fo export from osm file to csv file : https://mygeodata.cloud/converter/osm-to-csv#:~:text=Upload%20your%20OSM%20data%20(widely,will%20be%20exported%20as%20well.
# 
# 
# 
#
# **Various Github references for OSM Wrangling 
# https://github.com/wblakecannon/udacity-dand/tree/master/4-Data-Wrangling  
# https://hadrien-lcrx.github.io/notebooks/Boston_Data_Wrangling.html  
# http://napitupulu-jon.appspot.com/posts/wrangling-openstreetmap.html  
# https://github.com/GloSanc/Wrangle_OSM
# 

# In[ ]:




