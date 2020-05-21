#!/usr/bin/env python3

'''
Author: Ronald Adomako
'''

import csv
import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when
import statsmodels.api as sm

def parseCL(idx,part):
    if idx == 0:
        next(part)
    for p in csv.reader(part):
        
        
        LL_HN = p[2]
        RH_HN = p[5]
        
        #ID
        if len(p[0])==0:
            continue
        else:
            ID = p[0].strip()
            
        try:
            symm = str(int(ID)%2)
        except:
            symm = '0'
            pass
        
        #Full_Street and Street Label:
        if len(p[28])==0 and len(p[10])==0:
            continue
        else:
            full_st = p[28].lower().strip()
            st_label = p[10].lower().strip()
        
        #borocode
        if p[13].strip() not in ['1','2','3','4','5']:
            continue
        borocode = p[13]
        
        #yield
        row = (ID, full_st, st_label, borocode, 
              LL_HN, RH_HN, symm )
        
        yield row
        
def parseCSV(idx,part):  
    county = {
        'BROOKLYN':('BK','K','KING','KINGS'),
        'MANHATTAN':('MAN','MH','MN','NEWY','NEW Y','NY'),
        'BRONX':('BRONX','BX'), #MATCH ON SUBSTRING
        'QUEENS': ('Q','QN','QNS','QU','QUEEN'),
        'STATEN ISLAND': ('R','RICHMOND')
    }

    counties ={}
    boros = list(county.keys())
    boros = [boro.lower() for boro in county.keys()]

    county_tups = list(county.values())
    for code in county_tups:
        for symb in code:
            counties[symb.lower()]=str(county_tups.index(code)+1)
        
    years = ['2015','2016','2017','2018','2019']
    
    if idx == 0:
        next(part)
    for p in csv.reader(part):
        if p[23].isalpha() and p[24]=='' and p[21]=='' and p[23]=='':
            continue
            
        HN = p[23].lower().strip()
        try:
            HNC = str(int(HN)%2)
        except:
            HNC ='0'
            pass
            
        SN = p[24].lower().strip()
        
        cn = p[21].lower().strip()
        if cn not in list(counties.keys()):
            next(part)
        try:    
            CN = counties[cn]
        except:
            continue
        
        year = p[4][-4:].lower().strip()
        
        #yield result
        row = (HN,HNC,SN,CN,year)
        
        if year in years:
            yield row
            
def save(rdd,outFolder):
    rdd.saveAsText(outFolder)
    return

def main():
    geo_file =  '/data/share/bdm/nyc_cscl.csv'
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    directory = '/data/share/bdm/nyc_parking_violation/'
    
    rows = sc.textFile(directory + 'nyc_parking_violation/201[5-9].csv')\
        .mapPartitionsWithIndex(parseCSV)
    tkts_Frame = sqlContext.createDataFrame(rows,('House Number','Symm_tkt','Street Name', 'County','Year'))
   
    labels=('ID','Full Street','st label','borocode','Low','High','Symm_CL')
    centerLine = sc.textFile(geo_file).mapPartitionsWithIndex(parseCL)
    CL_frame = sqlContext.createDataFrame(centerLine,labels)
    
    print(f'\n\n\nYou have {centerLine.count()} in your CenterLine data\n\n\n')
  
    tkts_Frame = tkts_Frame.withColumn('House Number',tkts_Frame['House Number'].cast('int'))
    #CL_frame = CL_frame.withColumn('ID',CL_frame['ID'].cast('int'))
    CL_frame = CL_frame.withColumn('Low',CL_frame['Low'].cast('int'))
    CL_frame = CL_frame.withColumn('High',CL_frame['High'].cast('int'))
    
    cond1 = CL_frame['Full Street'] == tkts_Frame['Street Name']
    cond2 = CL_frame['st label'] == tkts_Frame['Street Name']
    cond3 = CL_frame['borocode'] == tkts_Frame['County']
    cond4 = CL_frame['High'] >= tkts_Frame['House Number']
    cond5 = CL_frame['Low'] <= tkts_Frame['House Number']
    cond6 = CL_frame['Symm_CL'] == tkts_Frame['Symm_tkt']

    df_match_1= CL_frame.join(tkts_Frame,cond3  & cond4 & cond5 & cond6 & cond2)
    df_match_2 = CL_frame.join(tkts_Frame, cond3 & cond4 & cond5 & cond6 & cond1)
    
    df_match =  df_match_1.union(df_match_2)
    total = df_match.count()
    print(f'\n\n\nYou have {total} total match in your Tickets data to NYC\n\n\n')

    group = df_match.groupby(['ID',"Year"]).count()
    counted = group.groupby(['ID']).pivot('Year').sum()
    counted = counted.na.fill(0)
    counted = counted.sort('ID')#.show(False)
    
    headers = ('ID','2015','2016','2017','2018','2019','COEF')
    new_rdd = counted.rdd.map(lambda row: (row[0], row[1], row[2], row[3], row[4],row[5],\
                                      sm.OLS(row[1:6], list(range(5))).fit().params[0]),headers)
    
    save(new_rdd,sys.argv[1])
    
if __name__ == '__main__':
    main()
    