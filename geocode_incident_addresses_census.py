# -*- coding: utf-8 -*-
"""
Geocode those with addresses.
"""
import pandas as pd
from pandas.io.json import json_normalize
import censusgeocode as cg
import math, os, geocoder, json, requests, datetime

# create function to call OSM and Bing
def callWSRBLocator(filename):

    print (datetime.datetime.now(),'    Geocoding addresses')
    t3 = datetime.datetime.now()
    
    df = pd.read_csv(filename)
    
    name, ext = os.path.splitext(filename)
    
    folder = r'\\FS-SEA-1\Protection_Data_Files\Projects\19_022_NFIRS_EDA\IL_wsrb\geocoded_results'
    parts = filename.split("\\")
    newpath = os.path.join(folder, parts[-1])
    name, ext = os.path.splitext(newpath)
    
    url = 'http://wa-app-prd-001.wsrb.com/AddressWebServiceInternal/api/address/SearchAddressSingleLine/'

    try:
        for i, row in df.iterrows():
            
            temp = df.loc[i,'addressall']
            
            payload=" '{}' ".format(temp)
            payload=payload.replace('0    ', ' ')
            payload=payload.replace('Name: singleaddress, dtype: object', '')

            headers = {    'content-type':'application/json',}
            response = requests.post(url, data=payload, headers=headers)
            response_dict = json.loads(response.text)
            df_temp=json_normalize(response_dict,'AddressGeocoded')
            
            if df_temp.empty or df_temp['AddressLocation'].empty:
                pass
            else:
                df.at[i, 'lat'] = df_temp.AddressLocation[0]['Y']
                df.at[i, 'lon'] = df_temp.AddressLocation[0]['X']
                df.at[i, 'confidence'] = df_temp.Confidence[0]
                df.at[i, 'source'] = df_temp['Source'][0]

    except Exception:
        print('Something went wrong')
    else:
        pass
    
    df.to_csv(name + '_latlong' + ext, header=True, index=False, float_format="%.6f")
    
    print (datetime.datetime.now(),'    Finished geocoding')
    t4 = datetime.datetime.now()
    
    return t3, t4, filename

def joinFrames(df_right):
    
    df_left = pd.read_csv(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\lastday_AccessComments.csv')
    
    df_join = pd.concat([df_left, df_right], sort=True)
    
    df_join.to_csv(r'C:\Users\zhuzhux\Desktop\19_017_userAccessCommentAddress\lastday_AccessComments_latlong.csv')

def callCensusBulkGeocoder(filename):

    print (datetime.datetime.now(),'    Geocoding addresses with Census Bulk')
    t5 = datetime.datetime.now()
    
    df = pd.read_csv(filename)
#    print(df.sample(n=1))
    
#    name, ext = os.path.splitext(filename)
    
    folder = r'\\fs-sea-1\Protection_Data_Files\Projects\19_022_NFIRS_EDA\IL\geocoded_results'
    parts = filename.split("\\")
    newpath = os.path.join(folder, parts[-1])
    name, ext = os.path.splitext(newpath)
    
    try:
        result = cg.addressbatch(filename)
#        print(result[0])
        df = pd.DataFrame(result)     
        
    except Exception:
        print('Something went wrong')
    else:
        pass
    
    #df.to_csv(name + '_bulk' + ext, header=True, index=False, float_format="%.6f")
    
    print (datetime.datetime.now(),'    Finished geocoding with Census Bulk Geocoder')
    
    t6 = datetime.datetime.now()
    return t5, t6, filename, name, ext, df
   
def callCensusSingleGeocoder(df):

    print (datetime.datetime.now(),'    Geocoding addresses with Census Single Geocoder')
    t7 = datetime.datetime.now()
    
#    print(filename)
#    df = pd.read_csv(filename)
#    print(df.sample(n=1))
#    print(df.head(n=1))
    
    try:
        #cg.address(street, city, state, zipcode)
#        df = pd.read_csv(filename, names=['AddressID', 'StreetAddress', 'CITY','STATE', 'ZIP5'])
        
        # using dataframe of all addresses
        for j, row in df.iterrows():
            if df.loc[j, 'match']:
                pass
            else:
                df.at[j, 'parsed'] = '-'
                #print(df.loc[j, 'address'])
                #payload=" '{}' ".format(df_temp.loc[j, 'address'])
                #cg.onelineaddress('1600 Pennsylvania Avenue, Washington, DC', returntype='locations')
                g = cg.onelineaddress(df.loc[j, 'address'], returntype='locations')
                #g = cg.onelineaddress(payload)
                if len(g)==0:
                    #print ('g is empty')
                    
                    pass   
                else:
#                    print(g[0]['coordinates']['x'], g[0]['coordinates']['y'] )
                    df.at[j, 'lat'] = g[0]['coordinates']['y']
                    df.at[j, 'lon'] = g[0]['coordinates']['x']
                    df.at[j, 'geocoder'] = 'Census Single'
                    df.at[j, 'parsed'] = g[0]['matchedAddress']
                    #df.at[j, 'match'] = True
        
    except Exception:
        print('Something went wrong')
    else:
        pass
    
    #    name, ext = os.path.splitext(filename)
    
#    folder = r'\\fs-sea-1\Protection_Data_Files\Projects\19_022_NFIRS_EDA\IL\geocoded_results'
#    parts = filename.split("\\")
#    newpath = os.path.join(folder, parts[-1])
#    name, ext = os.path.splitext(newpath)
    
#    df.to_csv(name + '_bulk_single' + ext, header=True, index=False, float_format="%.6f")
    
    df.to_csv(r'\\fs-sea-1\Protection_Data_Files\Projects\19_022_NFIRS_EDA\IL\geocoded_results\before_osm_bing.csv')
    print (datetime.datetime.now(),'    Finished geocoding')
    
    t8 = datetime.datetime.now()
    return t7, t8, df

def callOSMBing(df):
    '''
    Geocode address using OSM, then use Bing
    '''
    print (datetime.datetime.now(),'    Geocoding addresses with OSM and Bing')
    t9 = datetime.datetime.now()    
    
    #----------OSM first, then Bing --------------------------------
    for j, row in df.iterrows():
        #if df.loc[j, 'parsed']:
        #if df.loc[j, 'geocoder'] == '':
        if df.at[j, 'parsed'] != '-':
            pass
        else:
            # using OSM to geocode
            # https://operations.osmfoundation.org/policies/nominatim/
            #print(df.loc[j, 'address'])
            g = geocoder.osm(df.loc[j, 'address'])
            if g:
                df.at[j, 'lat'] = g.osm['y']
                df.at[j, 'lon'] = g.osm['x']
                df.at[j, 'geocoder'] = 'OSM'
                df.at[j, 'parsed'] = g.json['address']           
                df.at[j, 'confidence'] = g.json['accuracy']
                #print('OSM')
            else:
                # using Bing to geocode
                # https://docs.microsoft.com/en-us/bingmaps/spatial-data-services/geocode-and-data-source-limits
                g = geocoder.bing(df.loc[j, 'address'], key='')
                df.at[j, 'lat'] = g.lat
                df.at[j, 'lon'] = g.lng
                df.at[j, 'geocoder'] = 'Bing'
                df.at[j, 'parsed'] = g.json['address']
                df.at[j, 'confidence'] = g.json['confidence']
                #print('bing')
        
        #print('parsed address: ', df.at[j, 'parsed'])
        #print('returned lat lon: ', df.at[j, 'lat'],df.at[j, 'lon'] )
    
    #df.to_csv(r'\\fs-sea-1\Protection_Data_Files\Projects\19_022_NFIRS_EDA\IL\geocoded_results\after_osm_bing.csv')
    print (datetime.datetime.now(),'    Finished geocoding')
    
    t10 = datetime.datetime.now()
    return t9, t10, df

def main():
    
    t1 = datetime.datetime.now()
    print (t1)
     
    directory = r'\\fs-sea-1\Protection_Data_Files\Projects\19_022_NFIRS_EDA\IL\split_files'
    
    data = []
    col_names = ['startTime', 'endTime', 'filename']
    
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            print(os.path.join(directory, filename))
            filename = os.path.join(directory, filename)

            # first pass through Census bulk geocoder
            t5, t6, filename, name, ext, df = callCensusBulkGeocoder(filename)
            data.append([t5, t6, name])
#            df.to_csv(name + '_2census_latlong' + ext, header=True, index=False, float_format="%.6f")
            
            # second pass through Census single address
            t7, t8, df = callCensusSingleGeocoder(df)
            data.append([t7, t8, name])
                        
#            # _____ pass through WSRB Locator, for Bing
#            t3, t4, df = callWSRBLocator(df)
#            data.append([t3, t4, name])
            
            # pass through OSM and Bing
            t9, t10, df = callOSMBing(df)
            data.append([t9, t10, name])            
            
        else:
            continue
        
        folder = r'\\fs-sea-1\Protection_Data_Files\Projects\19_022_NFIRS_EDA\IL\geocoded_results'
        parts = filename.split("\\")
        newpath = os.path.join(folder, parts[-1])
        name, ext = os.path.splitext(newpath)
        df.to_csv(name + '_bulk_single_osmbing' + ext, header=True, index=False, float_format="%.6f")
    
    df_time = pd.DataFrame(data, columns = col_names)
    
    df_time.to_csv(r'\\fs-sea-1\Protection_Data_Files\Projects\19_022_NFIRS_EDA\IL\run_times.csv')    
    
    #joinFrames(df)
    
    # print date time after processing
    t2 = datetime.datetime.now()
    #find total time elapsed
    print ('total run time:               ', t2 - t1)

if __name__ == '__main__':
    main()