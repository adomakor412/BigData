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
        
        #Left Houses
        if '-' in p[2] and '-' in p[3]:#need and statement otherwise may run into case with nulls
            V1_LL, V2_LL = p[2].strip().split('-')
            LL_HN = V1_LL#(V1_LL, V2_LL)
            
            V1_LH, V2_LH = p[3].strip().split('-')
            LH_HN = V1_LH#(V1_LH, V2_LH)
            
        elif len(p[2])==0  and len(p[3])==0:
            LL_HN, LH_HN = '0', '0'
            
        else:
            LL_HN, LH_HN = p[2].split('-')[0],  p[3].split('-')[0]
            
        #Right Houses
        if '-' in p[4] and '-' in p[5]:
            V1_RL, V2_RL = p[4].strip().split('-')
            RL_HN = V1_RL#(V1_RL, V2_RL)
            
            V1_RH, V2_RH = p[5].strip().split('-')
            RH_HN = V1_RH#(V1_RH, V2_RH)
            
        elif len(p[4])==0 and len(p[5])==0:
            RL_HN, RH_HN = '0', '0'
            
        else:
            RL_HN, RH_HN = p[4].split('-')[0], p[5].split('-')[0]
        
        #ID
        if len(p[0])==0:
            continue
        else:
            ID = p[0].strip()
        
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
              LL_HN, LH_HN, RL_HN, RH_HN )
        
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
    
        if '-' in p[23]:
            HN = p[23].split('-')[0].lower().strip()
            HNC = p[23].split('-')[1].lower().strip()
        
        elif '' == p[23]:
            continue
        else:
            HN = p[23].lower().strip()
            HNC = ""
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
            
def save(coeff,aList,outPath):
    text_file = open(outPath, "w")
    n = text_file.write(f'{coeff} {aList[0]} {aList[1]} {aList[2]} {aList[3]} {aList[4]}')
    text_file.close()
    return

def nycl():
    return

def main():
    geo_file =  '/data/share/bdm/nyc_cscl.csv'
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    directory = '/data/share/bdm/nyc_parking_violation/'
    
    labels=('ID','Full Street','st label','borocode','LL_HN','LH_HN','RL_HN','RH_HN')
    centerLine = sc.textFile(geo_file).mapPartitionsWithIndex(parseCL)
    print(f'\n\n\nYou have {centerLine.count()} in your CenterLine data\n\n\n')
    CL_frame = sqlContext.createDataFrame(centerLine,labels)
    
    rows = sc.textFile(directory+'201[5-9].csv').mapPartitionsWithIndex(parseCSV)
    tkts_Frame = sqlContext.createDataFrame(rows,('House Number','HN Compound','Street Name', 'County','Year'))
    
    tkts_Frame = tkts_Frame.withColumn('House Number',tkts_Frame['House Number'].cast('int'))
    tkts_Frame = tkts_Frame.withColumn('HN Compound',tkts_Frame['HN Compound'].cast('int'))
    
    tkts_HN_even = tkts_Frame.filter(tkts_Frame['House Number']%2==0)
    tkts_HN_odd = tkts_Frame.filter(tkts_Frame['House Number']%2==1)
    
    #####EVEN Conditions ########
    cond1 = CL_frame['Full Street'] == tkts_HN_even['Street Name']
    cond2 = CL_frame['st label'] == tkts_HN_even['Street Name']
    cond3 = CL_frame['borocode'] == tkts_HN_even['County']
    cond4 = CL_frame['RH_HN'] >= tkts_HN_even['House Number']
    cond5 = CL_frame['RL_HN'] <= tkts_HN_even['House Number']

    #####ODD Conditions ########
    cond6 = CL_frame['Full Street'] == tkts_HN_odd['Street Name']
    cond7 = CL_frame['st label'] == tkts_HN_odd['Street Name']
    cond8 = CL_frame['borocode'] == tkts_HN_odd['County']
    cond9 = CL_frame['LH_HN'] >= tkts_HN_odd['House Number']
    cond10 = CL_frame['LL_HN'] <= tkts_HN_odd['House Number']
    
    df_match_3145 = CL_frame.join(tkts_HN_even,cond3 & cond1 & cond4 & cond5 )
    a = df_match_3145.count()
    
    df_match_3245 = CL_frame.join(tkts_HN_even,cond3 & cond2 & cond4 & cond5 )
    b = df_match_3245.count()
    
    df_match_869_10 = CL_frame.join(tkts_HN_odd,cond8 & cond6 & cond9 & cond10 )
    c = df_match_869_10.count()
    
    df_match_879_10 = CL_frame.join(tkts_HN_odd,cond8 & cond7 & cond9 & cond10 )
    d = df_match_879_10.count()
    
    total = a + b + c + d
    print(f'\n\n\nYou have {total} total match in your Tickets data to NYC\n\n\n')
    
    grouped_even = df_match_3245.union(df_match_3145)
    grouped_odd = df_match_869_10.union(df_match_879_10)

    grouped = grouped_odd.union(grouped_even)
    group = grouped.groupby(['ID',"Year"]).count()
    
    uniq_id = group.count()
    
    counted = group.groupby(['ID']).pivot('Year').sum()
    counted = counted.na.fill(0)
    
    years = ['2015','2016','2017','2018','2019']
    OLS = counted.groupBy().sum().collect()
    
    y = [ i for i in OLS[0]]
    model = sm.OLS(y, list(range(5)))
    results = model.fit()
    
    save(uniq_id, OLS[0], sys.argv[1])
    
if __name__ == '__main__':
    main()
    