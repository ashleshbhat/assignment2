#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ==================================
#
# program to read in several webpages from www.oldbaileyonline.org/
# in order to create a large data set
# specify the number of pages to read by modifying variable num_pages 
#
num_pages = 1000  # number of pages to read
#  ==================================
# importing libraries
from urllib.request import urlopen
from html.parser import HTMLParser
from bs4 import BeautifulSoup as bs
import re, time, psutil, os, sys
from functools import reduce
from StopWords import clean_stopwords

# ================================
# Functions
def remove_duplicates(in_list):
    out_list =[]
    for val in in_list:
        if not val in out_list:
            out_list.append(val)
    return out_list

# -------------------------------- =
def extract_text(stringSoup, Container):
    formTag = stringSoup.find("form")
    # access main2 and get the links
    for link in formTag.find_all('a'):
        string = link.get('href').replace("\n","")
        newTab = urlopen("https://www.oldbaileyonline.org/" + string)
        Container += str(bs(newTab.read(), "html.parser").find(id="main2").get_text().lower())
    return Container

# ================================
# string to store the text
textContainer = ""

# MemBeforeReadUrl = psutil.Process(os.getpid()).memory_info()
# total = time.time() # start total time counter
Url = "https://www.oldbaileyonline.org/search.jsp?form=searchHomePage&_divs_fulltext=Smith&kwparse=and&_persNames_surname=&_persNames_given=&_persNames_alias=&_offences_offenceCategory_offenceSubcategory=&_verdicts_verdictCategory_verdictSubcategory=&_punishments_punishmentCategory_punishmentSubcategory=&_divs_div0Type_div1Type=&fromMonth=07&fromYear=1749&toMonth=01&toYear=1913&ref=&submit.x=0&submit.y=0&submit=Search"

# read in link
InputUrl = urlopen(Url)
InputText = InputUrl.read()

print("Started reading URL .....")
for i in range(1,num_pages+1):
    print (i,"/",num_pages)
    # use BeautifulSoup to strip HTML tags
    soup = bs(InputText, "html.parser").find(id="main2")
    textContainer = extract_text(soup, textContainer) #.encode('utf-8')
    # get nextlink page
    nextLink = soup.find("li",{"class" : "last"}).find('a').get('href')
    
    nextPage = urlopen("https://www.oldbaileyonline.org/"+nextLink)
    InputText = nextPage.read()

textContainer = clean_stopwords(textContainer)
textContainer = str(re.compile('[a-zA-Z]+').findall(textContainer))[1:-1] #remobe brackets
textContainer = textContainer
textfile = open("Output1000.txt", "w+")
# use unicode for OS X
if sys.platform == 'darwin':
    textContainer = str(textContainer.encode('utf-8'))
    
textfile.write(textContainer.replace("', '"," ").replace("'",""))
textfile.close()
print ("text file written")
