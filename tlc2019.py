import geopandas as gpd
import pandas as pd
import numpy as np
import shapely
import re
import datetime
from shapely import wkt
from geosupport import Geosupport



pd.set_option('display.max_columns', None)
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/ONSTPARKING/'
#path='C:/Users/Y_Ma2/Desktop/ONSTPARKING/'
#path='E:/ONSTPARKING/'
#path='D:/ONSTPARKING/'
#path='I:/ONSTPARKING/'
path='/home/mayijun/ONSTPARKING/'
#path='C:/Users/y_ma2/OneDrive - NYC O365 HOSTED/ONSTPARKING/'


## Calculate PAVEMENTEDGE bearing
#def calcpvmtbearing(pvbr):
#    latfrom=np.radians(pvbr['latfrom'])
#    latto=np.radians(pvbr['latto'])
#    difflong=np.radians(pvbr['longto']-pvbr['longfrom'])
#    pvbearing=np.degrees(np.arctan2(np.sin(difflong)*np.cos(latfrom),
#                                      np.cos(latfrom)*np.sin(latto)-(np.sin(latfrom)*np.cos(latto)*np.cos(difflong))))
#    pvbr['pvbearing']=pvbearing
#    return pvbr

## Calculate PAVEMENTEDGE direction
#def calcpvmtdir(pvdir):
#    if (pvdir['pvbearing']>=0)&(pvdir['pvbearing']<90):
#        pvdir['pvdir']='N/E'
#    elif (pvdir['pvbearing']>=90)&(pvdir['pvbearing']<=180):
#        pvdir['pvdir']='S/E'
#    elif (pvdir['pvbearing']<0)&(pvdir['pvbearing']>=-90):
#        pvdir['pvdir']='N/W'
#    elif (pvdir['pvbearing']<-90)&(pvdir['pvbearing']>=-180):
#        pvdir['pvdir']='S/W'
#    return pvdir

## Adjust HYDRANT to closest blockface
#def adjhydrant(hd):
#    global pvmtedgemdn
#    global hydrantbuffer
#    hdbk=pvmtedgemdn[np.isin(pvmtedgemdn['bkfaceid'],hydrantbuffer.loc[hydrantbuffer['unitid']==hd['unitid'],'bkfaceid'])].reset_index(drop=True)
#    if len(hdbk)>0:
#        hd['bkfaceid']=hdbk.loc[np.argmin([hd['geometry'].distance(x) for x in hdbk['geometry']]),'bkfaceid']
#        hd['snapdist']=min([hd['geometry'].distance(x) for x in hdbk['geometry']])
#        hd['adjgeom']=shapely.ops.nearest_points(hd['geometry'],hdbk.loc[np.argmin([hd['geometry'].distance(x) for x in hdbk['geometry']]),'geometry'])[1].wkt
#    else:
#        print(str(hd['unitid'])+' no bkfaceid joined')
#    return hd

## Separate DOTREG SIGN description and MUTCD
#def sepdescmutcd(dm):
#    if pd.isna(dm['Sign_description']):
#        dm['desc']=''
#        dm['mutcd']=' '.join(dm['SR_Mutcd_Code'].split()).upper()
#    elif pd.isna(dm['Unnamed: 7']):
#        dm['desc']=' '.join(dm['Sign_description'].split()).upper()
#        dm['mutcd']=' '.join(dm['SR_Mutcd_Code'].split()).upper()
#    elif pd.isna(dm['Unnamed: 8']):
#        dm['desc']=' '.join((dm['Sign_description']+', '+dm['SR_Mutcd_Code']).split()).upper()
#        dm['mutcd']=' '.join(dm['Unnamed: 7'].split()).upper()
#    else:
#        dm['desc']=' '.join((dm['Sign_description']+', '+dm['SR_Mutcd_Code']+', '+dm['Unnamed: 7']).split()).upper()
#        dm['mutcd']=' '.join(dm['Unnamed: 8'].split()).upper()
#    return dm

## Separate DOTREG SIGN description
#def sepdesc(sds):
#    if sds['descnum']==0:
#        sds['desc1']=' '.join(sds['desc'].split()).upper()
#        sds['dir1']=np.where(len(list(re.finditer('ARROW',sds['desc1'])))>0,1,2)
#    elif sds['descnum']==1:
#        descarrow=[x.start() for x in list(re.finditer('>',sds['desc']))]
#        sds['desc1']=' '.join(sds['desc'][:descarrow[0]+1].split()).upper()
#        sds['dir1']=np.where(len(list(re.finditer('<',sds['desc1'])))>0,2,1)
#        sds['desctail']=' '.join(sds['desc'][descarrow[0]+1:].split()).upper()
#    elif sds['descnum']==2:
#        descarrow=[x.start() for x in list(re.finditer('>',sds['desc']))]
#        sds['desc1']=' '.join(sds['desc'][:descarrow[0]+1].split()).upper()
#        sds['dir1']=np.where(len(list(re.finditer('<',sds['desc1'])))>0,2,1)
#        sds['desc2']=' '.join(sds['desc'][descarrow[0]+1:descarrow[1]+1].split()).upper()
#        sds['dir2']=np.where(len(list(re.finditer('<',sds['desc2'])))>0,2,1)
#        sds['desctail']=' '.join(sds['desc'][descarrow[1]+1:].split()).upper()
#    elif sds['descnum']==3:
#        descarrow=[x.start() for x in list(re.finditer('>',sds['desc']))]
#        sds['desc1']=' '.join(sds['desc'][:descarrow[0]+1].split()).upper()
#        sds['dir1']=np.where(len(list(re.finditer('<',sds['desc1'])))>0,2,1)
#        sds['desc2']=' '.join(sds['desc'][descarrow[0]+1:descarrow[1]+1].split()).upper()
#        sds['dir2']=np.where(len(list(re.finditer('<',sds['desc2'])))>0,2,1)
#        sds['desc3']=' '.join(sds['desc'][descarrow[1]+1:descarrow[2]+1].split()).upper()
#        sds['dir3']=np.where(len(list(re.finditer('<',sds['desc3'])))>0,2,1)
#        sds['desctail']=' '.join(sds['desc'][descarrow[2]+1:].split()).upper()
#    return sds

## Extract days from DOTREG SIGN MUTCD description
#def extractdays(ed):
#    ed=ed.copy()
#    if pd.notna(re.search('ANYTIME',ed['time'])):
#        ed[['m','t','w','r','f','s','u']]+=1
#        ed['daysflag']+=1  
#    if pd.notna(re.search('ALL DAYS',ed['time'])):
#        ed[['m','t','w','r','f','s','u']]=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('HOTEL LOADING ZONE',ed['time'])):
#        ed[['m','t','w','r','f','s','u']]+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('FIRE ZONE',ed['time'])):
#        ed[['m','t','w','r','f','s','u']]+=1
#        ed['daysflag']+=1        
#    if ed['time'].replace('<','').replace('>','').replace('-','').replace(' ','')=='':
#        ed[['m','t','w','r','f','s','u']]+=1
#        ed['daysflag']+=1    
#    if pd.notna(re.search('SCHOOL DAYS',ed['time'])):
#        ed[['m','t','w','r','f','s','u']]+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('MONDAY',ed['time'])):
#        ed['m']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('MON',ed['time'])):
#        ed['m']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('TUESDAY',ed['time'])):
#        ed['t']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('TUES',ed['time'])):
#        ed['t']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('WEDNESDAY',ed['time'])):
#        ed['w']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('WED',ed['time'])):
#        ed['w']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('THURSDAY',ed['time'])):
#        ed['r']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('THURS',ed['time'])):
#        ed['r']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('FRIDAY',ed['time'])):
#        ed['f']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('FRI',ed['time'])):
#        ed['f']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('SATURDAY',ed['time'])):
#        ed['s']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('SAT',ed['time'])):
#        ed['s']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('SUNDAY',ed['time'])):
#        ed['u']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('SUN',ed['time'])):
#        ed['u']+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('MONDAY-FRIDAY',ed['time'])):
#        ed[['m','t','w','r','f']]+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('MON-FRI',ed['time'])):
#        ed[['m','t','w','r','f']]+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('MON THRU FRI',ed['time'])):
#        ed[['m','t','w','r','f']]+=1
#        ed['daysflag']+=1
#    if pd.notna(re.search('EXCEPT SUNDAY',ed['time'])):
#        ed[['m','t','w','r','f','s']]+=1
#        ed['u']=0
#        ed['daysflag']+=1
#    if pd.notna(re.search('EXCEPT SATURDAY',ed['time'])):
#        ed[['m','t','w','r','f','u']]+=1
#        ed['s']=0
#        ed['daysflag']+=1
#    if pd.notna(re.search('INCLUDING SUNDAY',ed['time'])):
#        ed[['m','t','w','r','f','s','u']]+=1
#        ed['daysflag']+=1
#    return ed

## Extract hours from DOTREG SIGN MUTCD description
#def extracthours(eh):
#    eh=eh.copy()
#    if pd.notna(re.search('\d+AM-\d+AM',eh['time'])):
#        eh['starthour']=str(int(re.search('\d+AM-\d+AM',eh['time'])[0].split('-')[0].replace('AM',''))).zfill(2)+':00'
#        eh['endhour']=str(int(re.search('\d+AM-\d+AM',eh['time'])[0].split('-')[1].replace('AM',''))).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('\d+AM-\d+PM',eh['time'])):
#        eh['starthour']=str(int(re.search('\d+AM-\d+PM',eh['time'])[0].split('-')[0].replace('AM',''))).zfill(2)+':00'
#        eh['endhour']=str(int(re.search('\d+AM-\d+PM',eh['time'])[0].split('-')[1].replace('PM',''))+12).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('\d+PM-\d+AM',eh['time'])):
#        eh['starthour']=str(int(re.search('\d+PM-\d+AM',eh['time'])[0].split('-')[0].replace('PM',''))+12).zfill(2)+':00'
#        eh['endhour']=str(int(re.search('\d+PM-\d+AM',eh['time'])[0].split('-')[1].replace('AM',''))).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('\d+PM-\d+PM',eh['time'])):
#        eh['starthour']=str(int(re.search('\d+PM-\d+PM',eh['time'])[0].split('-')[0].replace('PM',''))+12).zfill(2)+':00'
#        eh['endhour']=str(int(re.search('\d+PM-\d+PM',eh['time'])[0].split('-')[1].replace('PM',''))+12).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('\d+-\d+AM',eh['time'])):
#        eh['starthour']=str(int(re.search('\d+-\d+AM',eh['time'])[0].split('-')[0])).zfill(2)+':00'
#        eh['endhour']=str(int(re.search('\d+-\d+AM',eh['time'])[0].split('-')[1].replace('AM',''))).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('\d+-\d+PM',eh['time'])):
#        eh['starthour']=str(int(re.search('\d+-\d+PM',eh['time'])[0].split('-')[0])+12).zfill(2)+':00'
#        eh['endhour']=str(int(re.search('\d+-\d+PM',eh['time'])[0].split('-')[1].replace('PM',''))+12).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('\d+AM-MIDNIGHT',eh['time'])):
#        eh['starthour']=str(int(re.search('\d+AM-MIDNIGHT',eh['time'])[0].split('-')[0].replace('AM',''))).zfill(2)+':00'
#        eh['endhour']=str(0).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('\d+PM-MIDNIGHT',eh['time'])):
#        eh['starthour']=str(int(re.search('\d+PM-MIDNIGHT',eh['time'])[0].split('-')[0].replace('PM',''))+12).zfill(2)+':00'
#        eh['endhour']=str(0).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('MIDNIGHT-\d+AM',eh['time'])):
#        eh['starthour']=str(0).zfill(2)+':00'
#        eh['endhour']=str(int(re.search('MIDNIGHT-\d+AM',eh['time'])[0].split('-')[1].replace('AM',''))).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('MIDNIGHT-\d+PM',eh['time'])):
#        eh['starthour']=str(0).zfill(2)+':00'
#        eh['endhour']=str(int(re.search('MIDNIGHT-\d+PM',eh['time'])[0].split('-')[1].replace('PM',''))+12).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('\d+AM-NOON',eh['time'])):
#        eh['starthour']=str(int(re.search('\d+AM-NOON',eh['time'])[0].split('-')[0].replace('AM',''))).zfill(2)+':00'
#        eh['endhour']=str(12).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('\d+PM-NOON',eh['time'])):
#        eh['starthour']=str(int(re.search('\d+PM-NOON',eh['time'])[0].split('-')[0].replace('PM',''))+12).zfill(2)+':00'
#        eh['endhour']=str(12).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('NOON-\d+AM',eh['time'])):
#        eh['starthour']=str(12).zfill(2)+':00'
#        eh['endhour']=str(int(re.search('NOON-\d+AM',eh['time'])[0].split('-')[1].replace('AM',''))).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('NOON-\d+PM',eh['time'])):
#        eh['starthour']=str(12).zfill(2)+':00'
#        eh['endhour']=str(int(re.search('NOON-\d+PM',eh['time'])[0].split('-')[1].replace('PM',''))+12).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('ANYTIME',eh['time'])):
#        eh['starthour']=str(0).zfill(2)+':00'
#        eh['endhour']=str(0).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('FIRE ZONE',eh['time'])):
#        eh['starthour']=str(0).zfill(2)+':00'
#        eh['endhour']=str(0).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if pd.notna(re.search('HOTEL LOADING ZONE',eh['time'])):
#        eh['starthour']=str(0).zfill(2)+':00'
#        eh['endhour']=str(0).zfill(2)+':00'
#        eh['hoursflag']+=1
#    if eh['time'].replace('<','').replace('>','').replace('-','').replace(' ','')=='':
#        eh['starthour']=str(0).zfill(2)+':00'
#        eh['endhour']=str(0).zfill(2)+':00'
#        eh['hoursflag']+=1
#    return eh

## Find parking blockface based on traffic direction and parking lane number
#def lionpkbkface(pb):
#    if pb['trafficdir']=='W':
#        if pb['parkinglane']==0:
#            pb['lparking']=0
#            pb['rparking']=0
#        elif pb['parkinglane']==1:
#            pb['lparking']=0.1
#            pb['rparking']=0.9
#        elif pb['parkinglane'] in [2,4]:
#            pb['lparking']=1
#            pb['rparking']=1
#    elif pb['trafficdir']=='A':
#        if pb['parkinglane']==0:
#            pb['lparking']=0
#            pb['rparking']=0
#        elif pb['parkinglane']==1:
#            pb['lparking']=0.9
#            pb['rparking']=0.1
#        elif pb['parkinglane'] in [2,4]:
#            pb['lparking']=1
#            pb['rparking']=1
#    elif pb['trafficdir']=='T':
#        if pb['parkinglane']==0:
#            pb['lparking']=0
#            pb['rparking']=0
#        elif pb['parkinglane']==1:
#            pb['lparking']=0.5
#            pb['rparking']=0.5
#        elif pb['parkinglane'] in [2,4]:
#            pb['lparking']=1
#            pb['rparking']=1
#    return pb

## Check the nodeidfrom and nodeidto for each physicalid
##cft=onfromto[onfromto['physicalid']==11676]
##cft=onfromto[onfromto['physicalid']==4091]
##cft=onfromto[onfromto['physicalid']==106102]
#def checkfromto(cft):
#    cft=cft.reset_index(drop=True)
#    try:
#        if len(cft)==1:
#            cft=cft[['physicalid','streetcode','nodeidfrom','nodeidto']].reset_index(drop=True)
#        else:
#            cftfrom=cft[['nodeidfrom']].reset_index(drop=True).rename(columns={'nodeidfrom':'nodeid'})
#            cftfrom['fromto']='from'
#            cftto=cft[['nodeidto']].reset_index(drop=True).rename(columns={'nodeidto':'nodeid'})
#            cftto['fromto']='to'
#            cftfromto=pd.concat([cftfrom,cftto],axis=0,ignore_index=True)
#            check=cftfromto.groupby('nodeid',as_index=False).agg('count').reset_index(drop=True)
#            check=check.groupby('fromto',as_index=False).agg('count').reset_index(drop=True)
#            if (len(check)==2)&(list((check.loc[check['fromto']==1,'nodeid']==2))[0])&(list((check.loc[check['fromto']==2,'nodeid']>0))[0]):
#                cftfromto=cftfromto.drop_duplicates('nodeid',keep=False).reset_index(drop=True)
#                cft.loc[0,'nodeidfrom']=list(cftfromto.loc[cftfromto['fromto']=='from','nodeid'])[0]
#                cft.loc[0,'nodeidto']=list(cftfromto.loc[cftfromto['fromto']=='to','nodeid'])[0]
#                cft=cft.loc[[0],['physicalid','streetcode','nodeidfrom','nodeidto']].reset_index(drop=True)
#            else:
#                print(cft.loc[0,'physicalid'])
#    except:
#        print(cft.loc[0,'physicalid'])
#    return cft

## Remove fromstreetcode same to onstreetcode
#def rmvfromst(rfs):
#    rfs=rfs.reset_index(drop=True)
#    if len(rfs)==1:
#        rfs=rfs.reset_index(drop=True)
#    else:
#        rfs=rfs[rfs['onstreetcode']!=rfs['fromstreetcode']].reset_index(drop=True)
#    return rfs

## Remove tostreetcode same to onstreetcode
#def rmvtost(rts):
#    rts=rts.reset_index(drop=True)
#    if len(rts)==1:
#        rts=rts.reset_index(drop=True)
#    else:
#        rts=rts[rts['onstreetcode']!=rts['tostreetcode']].reset_index(drop=True)
#    return rts

## Combine two segments
##tsg=dotreglocclean[dotreglocclean['order']=='P-00180350']
##tsg=dotreglocclean[dotreglocclean['order']=='S-01305669']
#def cmbtwosegs(tsg):
#    global onfromtowa
#    tsg=tsg.reset_index(drop=True)
#    if len(tsg[tsg['flag']==1])==0:
#        twosegs1=onfromtowa[(onfromtowa['onstreetcode']==tsg.loc[0,'onstreetcode'])&
#                            (onfromtowa['fromstreetcode']==tsg.loc[0,'fromstreetcode'])]
#        twosegs2=onfromtowa[(onfromtowa['onstreetcode']==tsg.loc[0,'onstreetcode'])&
#                            (onfromtowa['tostreetcode']==tsg.loc[0,'tostreetcode'])]
#        twosegs=pd.merge(twosegs1,twosegs2,how='inner',left_on='nodeidto',right_on='nodeidfrom')
#        twosegs=twosegs[['physicalid_x','nodeidfrom_x','latfrom_x','longfrom_x','nodeidto_x','latto_x','longto_x','geodir_x',
#                         'physicalid_y','nodeidfrom_y','latfrom_y','longfrom_y','nodeidto_y','latto_y','longto_y','geodir_y']]
#        twosegs.columns=['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                         'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2']
#        twosegs=twosegs.drop_duplicates(keep='first').reset_index(drop=True)
#        twosegs['check']=twosegs[['physicalid1','physicalid2']].apply(lambda x:len(x.unique()),axis=1)
#        twosegs=twosegs[twosegs['check']==2].reset_index(drop=True)
#        if len(twosegs)>0:
#            tsg=pd.concat([tsg[tsg.index==0]]*len(twosegs),ignore_index=True)
#            for i in twosegs.index:
#                tsg.loc[i,'physicalid1']=twosegs.loc[i,'physicalid1']
#                tsg.loc[i,'nodeidfrom1']=twosegs.loc[i,'nodeidfrom1']
#                tsg.loc[i,'latfrom1']=twosegs.loc[i,'latfrom1']
#                tsg.loc[i,'longfrom1']=twosegs.loc[i,'longfrom1']
#                tsg.loc[i,'nodeidto1']=twosegs.loc[i,'nodeidto1']
#                tsg.loc[i,'latto1']=twosegs.loc[i,'latto1']
#                tsg.loc[i,'longto1']=twosegs.loc[i,'longto1']
#                tsg.loc[i,'geodir1']=twosegs.loc[i,'geodir1']
#                tsg.loc[i,'physicalid2']=twosegs.loc[i,'physicalid2']
#                tsg.loc[i,'nodeidfrom2']=twosegs.loc[i,'nodeidfrom2']
#                tsg.loc[i,'latfrom2']=twosegs.loc[i,'latfrom2']
#                tsg.loc[i,'longfrom2']=twosegs.loc[i,'longfrom2']
#                tsg.loc[i,'nodeidto2']=twosegs.loc[i,'nodeidto2']
#                tsg.loc[i,'latto2']=twosegs.loc[i,'latto2']
#                tsg.loc[i,'longto2']=twosegs.loc[i,'longto2']
#                tsg.loc[i,'geodir2']=twosegs.loc[i,'geodir2']
#                tsg.loc[i,'flag']=2
#    return tsg

## Combine three segments
##thsg=dotreglocclean[dotreglocclean['order']=='P-01252957']
#def cmbthreesegs(thsg):
#    global onfromtowa
#    thsg=thsg.reset_index(drop=True)
#    if len(thsg[(thsg['flag']==1)|(thsg['flag']==2)])==0:
#        threesegs1=onfromtowa[(onfromtowa['onstreetcode']==thsg.loc[0,'onstreetcode'])&
#                              (onfromtowa['fromstreetcode']==thsg.loc[0,'fromstreetcode'])]
#        threesegs3=onfromtowa[(onfromtowa['onstreetcode']==thsg.loc[0,'onstreetcode'])&
#                              (onfromtowa['tostreetcode']==thsg.loc[0,'tostreetcode'])]
#        threesegs2=onfromtowa[(onfromtowa['onstreetcode']==thsg.loc[0,'onstreetcode'])&
#                              (np.isin(onfromtowa['fromstreetcode'],threesegs1['tostreetcode'].unique()))&
#                              (np.isin(onfromtowa['tostreetcode'],threesegs3['fromstreetcode'].unique()))]
#        threesegs=pd.merge(threesegs1,threesegs2,how='inner',left_on='nodeidto',right_on='nodeidfrom')
#        threesegs=threesegs[['physicalid_x','nodeidfrom_x','latfrom_x','longfrom_x','nodeidto_x','latto_x','longto_x','geodir_x',
#                             'physicalid_y','nodeidfrom_y','latfrom_y','longfrom_y','nodeidto_y','latto_y','longto_y','geodir_y']]
#        threesegs.columns=['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                           'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2']
#        threesegs=pd.merge(threesegs,threesegs3,how='inner',left_on='nodeidto2',right_on='nodeidfrom')
#        threesegs=threesegs[['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                             'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2',
#                             'physicalid','nodeidfrom','latfrom','longfrom','nodeidto','latto','longto','geodir']]
#        threesegs.columns=['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                           'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2',
#                           'physicalid3','nodeidfrom3','latfrom3','longfrom3','nodeidto3','latto3','longto3','geodir3']
#        threesegs=threesegs.drop_duplicates(keep='first').reset_index(drop=True)
#        threesegs['check']=threesegs[['physicalid1','physicalid2','physicalid3']].apply(lambda x:len(x.unique()),axis=1)
#        threesegs=threesegs[threesegs['check']==3].reset_index(drop=True)
#        if len(threesegs)>0:
#            thsg=pd.concat([thsg[thsg.index==0]]*len(threesegs),ignore_index=True)
#            for i in threesegs.index:
#                thsg.loc[i,'physicalid1']=threesegs.loc[i,'physicalid1']
#                thsg.loc[i,'nodeidfrom1']=threesegs.loc[i,'nodeidfrom1']
#                thsg.loc[i,'latfrom1']=threesegs.loc[i,'latfrom1']
#                thsg.loc[i,'longfrom1']=threesegs.loc[i,'longfrom1']
#                thsg.loc[i,'nodeidto1']=threesegs.loc[i,'nodeidto1']
#                thsg.loc[i,'latto1']=threesegs.loc[i,'latto1']
#                thsg.loc[i,'longto1']=threesegs.loc[i,'longto1']
#                thsg.loc[i,'geodir1']=threesegs.loc[i,'geodir1']
#                thsg.loc[i,'physicalid2']=threesegs.loc[i,'physicalid2']
#                thsg.loc[i,'nodeidfrom2']=threesegs.loc[i,'nodeidfrom2']
#                thsg.loc[i,'latfrom2']=threesegs.loc[i,'latfrom2']
#                thsg.loc[i,'longfrom2']=threesegs.loc[i,'longfrom2']
#                thsg.loc[i,'nodeidto2']=threesegs.loc[i,'nodeidto2']
#                thsg.loc[i,'latto2']=threesegs.loc[i,'latto2']
#                thsg.loc[i,'longto2']=threesegs.loc[i,'longto2']
#                thsg.loc[i,'geodir2']=threesegs.loc[i,'geodir2']
#                thsg.loc[i,'physicalid3']=threesegs.loc[i,'physicalid3']
#                thsg.loc[i,'nodeidfrom3']=threesegs.loc[i,'nodeidfrom3']
#                thsg.loc[i,'latfrom3']=threesegs.loc[i,'latfrom3']
#                thsg.loc[i,'longfrom3']=threesegs.loc[i,'longfrom3']
#                thsg.loc[i,'nodeidto3']=threesegs.loc[i,'nodeidto3']
#                thsg.loc[i,'latto3']=threesegs.loc[i,'latto3']
#                thsg.loc[i,'longto3']=threesegs.loc[i,'longto3']
#                thsg.loc[i,'geodir3']=threesegs.loc[i,'geodir3']
#                thsg.loc[i,'flag']=3
#    return thsg

## Combine four segments
##fsg=dotreglocclean[dotreglocclean['order']=='P-01250627']
#def cmbfoursegs(fsg):
#    global onfromtowa
#    fsg=fsg.reset_index(drop=True)
#    if len(fsg[(fsg['flag']==1)|(fsg['flag']==2)|(fsg['flag']==3)])==0:
#        foursegs1=onfromtowa[(onfromtowa['onstreetcode']==fsg.loc[0,'onstreetcode'])&
#                             (onfromtowa['fromstreetcode']==fsg.loc[0,'fromstreetcode'])]
#        foursegs4=onfromtowa[(onfromtowa['onstreetcode']==fsg.loc[0,'onstreetcode'])&
#                             (onfromtowa['tostreetcode']==fsg.loc[0,'tostreetcode'])]
#        foursegs2=onfromtowa[(onfromtowa['onstreetcode']==fsg.loc[0,'onstreetcode'])&
#                             (np.isin(onfromtowa['fromstreetcode'],foursegs1['tostreetcode'].unique()))]
#        foursegs3=onfromtowa[(onfromtowa['onstreetcode']==fsg.loc[0,'onstreetcode'])&
#                             (np.isin(onfromtowa['fromstreetcode'],foursegs2['tostreetcode'].unique()))&
#                             (np.isin(onfromtowa['tostreetcode'],foursegs4['fromstreetcode'].unique()))]
#        foursegs=pd.merge(foursegs1,foursegs2,how='inner',left_on='nodeidto',right_on='nodeidfrom')
#        foursegs=foursegs[['physicalid_x','nodeidfrom_x','latfrom_x','longfrom_x','nodeidto_x','latto_x','longto_x','geodir_x',
#                           'physicalid_y','nodeidfrom_y','latfrom_y','longfrom_y','nodeidto_y','latto_y','longto_y','geodir_y']]
#        foursegs.columns=['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                          'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2']
#        foursegs=pd.merge(foursegs,foursegs3,how='inner',left_on='nodeidto2',right_on='nodeidfrom')
#        foursegs=foursegs[['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                           'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2',
#                           'physicalid','nodeidfrom','latfrom','longfrom','nodeidto','latto','longto','geodir']]
#        foursegs.columns=['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                          'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2',
#                          'physicalid3','nodeidfrom3','latfrom3','longfrom3','nodeidto3','latto3','longto3','geodir3']
#        foursegs=pd.merge(foursegs,foursegs4,how='inner',left_on='nodeidto3',right_on='nodeidfrom')
#        foursegs=foursegs[['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                           'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2',
#                           'physicalid3','nodeidfrom3','latfrom3','longfrom3','nodeidto3','latto3','longto3','geodir3',
#                           'physicalid','nodeidfrom','latfrom','longfrom','nodeidto','latto','longto','geodir']]
#        foursegs.columns=['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                          'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2',
#                          'physicalid3','nodeidfrom3','latfrom3','longfrom3','nodeidto3','latto3','longto3','geodir3',
#                          'physicalid4','nodeidfrom4','latfrom4','longfrom4','nodeidto4','latto4','longto4','geodir4']
#        foursegs=foursegs.drop_duplicates(keep='first').reset_index(drop=True)
#        foursegs['check']=foursegs[['physicalid1','physicalid2','physicalid3','physicalid4']].apply(lambda x:len(x.unique()),axis=1)
#        foursegs=foursegs[foursegs['check']==4].reset_index(drop=True)
#        if len(foursegs)>0:
#            fsg=pd.concat([fsg[fsg.index==0]]*len(foursegs),ignore_index=True)
#            for i in foursegs.index:
#                fsg.loc[i,'physicalid1']=foursegs.loc[i,'physicalid1']
#                fsg.loc[i,'nodeidfrom1']=foursegs.loc[i,'nodeidfrom1']
#                fsg.loc[i,'latfrom1']=foursegs.loc[i,'latfrom1']
#                fsg.loc[i,'longfrom1']=foursegs.loc[i,'longfrom1']
#                fsg.loc[i,'nodeidto1']=foursegs.loc[i,'nodeidto1']
#                fsg.loc[i,'latto1']=foursegs.loc[i,'latto1']
#                fsg.loc[i,'longto1']=foursegs.loc[i,'longto1']
#                fsg.loc[i,'geodir1']=foursegs.loc[i,'geodir1']
#                fsg.loc[i,'physicalid2']=foursegs.loc[i,'physicalid2']
#                fsg.loc[i,'nodeidfrom2']=foursegs.loc[i,'nodeidfrom2']
#                fsg.loc[i,'latfrom2']=foursegs.loc[i,'latfrom2']
#                fsg.loc[i,'longfrom2']=foursegs.loc[i,'longfrom2']
#                fsg.loc[i,'nodeidto2']=foursegs.loc[i,'nodeidto2']
#                fsg.loc[i,'latto2']=foursegs.loc[i,'latto2']
#                fsg.loc[i,'longto2']=foursegs.loc[i,'longto2']
#                fsg.loc[i,'geodir2']=foursegs.loc[i,'geodir2']
#                fsg.loc[i,'physicalid3']=foursegs.loc[i,'physicalid3']
#                fsg.loc[i,'nodeidfrom3']=foursegs.loc[i,'nodeidfrom3']
#                fsg.loc[i,'latfrom3']=foursegs.loc[i,'latfrom3']
#                fsg.loc[i,'longfrom3']=foursegs.loc[i,'longfrom3']
#                fsg.loc[i,'nodeidto3']=foursegs.loc[i,'nodeidto3']
#                fsg.loc[i,'latto3']=foursegs.loc[i,'latto3']
#                fsg.loc[i,'longto3']=foursegs.loc[i,'longto3']
#                fsg.loc[i,'geodir3']=foursegs.loc[i,'geodir3']
#                fsg.loc[i,'physicalid4']=foursegs.loc[i,'physicalid4']
#                fsg.loc[i,'nodeidfrom4']=foursegs.loc[i,'nodeidfrom4']
#                fsg.loc[i,'latfrom4']=foursegs.loc[i,'latfrom4']
#                fsg.loc[i,'longfrom4']=foursegs.loc[i,'longfrom4']
#                fsg.loc[i,'nodeidto4']=foursegs.loc[i,'nodeidto4']
#                fsg.loc[i,'latto4']=foursegs.loc[i,'latto4']
#                fsg.loc[i,'longto4']=foursegs.loc[i,'longto4']
#                fsg.loc[i,'geodir4']=foursegs.loc[i,'geodir4']
#                fsg.loc[i,'flag']=4
#    return fsg

## Combine five segments
##fvsg=dotreglocclean[dotreglocclean['order']=='P-01280281']
#def cmbfivesegs(fvsg):
#    global onfromtowa
#    fvsg=fvsg.reset_index(drop=True)
#    if len(fvsg[(fvsg['flag']==1)|(fvsg['flag']==2)|(fvsg['flag']==3)|(fvsg['flag']==4)])==0:
#        fivesegs1=onfromtowa[(onfromtowa['onstreetcode']==fvsg.loc[0,'onstreetcode'])&
#                             (onfromtowa['fromstreetcode']==fvsg.loc[0,'fromstreetcode'])]
#        fivesegs5=onfromtowa[(onfromtowa['onstreetcode']==fvsg.loc[0,'onstreetcode'])&
#                             (onfromtowa['tostreetcode']==fvsg.loc[0,'tostreetcode'])]
#        fivesegs2=onfromtowa[(onfromtowa['onstreetcode']==fvsg.loc[0,'onstreetcode'])&
#                             (np.isin(onfromtowa['fromstreetcode'],fivesegs1['tostreetcode'].unique()))]
#        fivesegs3=onfromtowa[(onfromtowa['onstreetcode']==fvsg.loc[0,'onstreetcode'])&
#                             (np.isin(onfromtowa['fromstreetcode'],fivesegs2['tostreetcode'].unique()))]
#        fivesegs4=onfromtowa[(onfromtowa['onstreetcode']==fvsg.loc[0,'onstreetcode'])&
#                             (np.isin(onfromtowa['fromstreetcode'],fivesegs3['tostreetcode'].unique()))&
#                             (np.isin(onfromtowa['tostreetcode'],fivesegs5['fromstreetcode'].unique()))]
#        fivesegs=pd.merge(fivesegs1,fivesegs2,how='inner',left_on='nodeidto',right_on='nodeidfrom')
#        fivesegs=fivesegs[['physicalid_x','nodeidfrom_x','latfrom_x','longfrom_x','nodeidto_x','latto_x','longto_x','geodir_x',
#                           'physicalid_y','nodeidfrom_y','latfrom_y','longfrom_y','nodeidto_y','latto_y','longto_y','geodir_y']]
#        fivesegs.columns=['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                          'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2']
#        fivesegs=pd.merge(fivesegs,fivesegs3,how='inner',left_on='nodeidto2',right_on='nodeidfrom')
#        fivesegs=fivesegs[['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                           'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2',
#                           'physicalid','nodeidfrom','latfrom','longfrom','nodeidto','latto','longto','geodir']]
#        fivesegs.columns=['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                          'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2',
#                          'physicalid3','nodeidfrom3','latfrom3','longfrom3','nodeidto3','latto3','longto3','geodir3']
#        fivesegs=pd.merge(fivesegs,fivesegs4,how='inner',left_on='nodeidto3',right_on='nodeidfrom')
#        fivesegs=fivesegs[['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                           'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2',
#                           'physicalid3','nodeidfrom3','latfrom3','longfrom3','nodeidto3','latto3','longto3','geodir3',
#                           'physicalid','nodeidfrom','latfrom','longfrom','nodeidto','latto','longto','geodir']]
#        fivesegs.columns=['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                          'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2',
#                          'physicalid3','nodeidfrom3','latfrom3','longfrom3','nodeidto3','latto3','longto3','geodir3',
#                          'physicalid4','nodeidfrom4','latfrom4','longfrom4','nodeidto4','latto4','longto4','geodir4']
#        fivesegs=pd.merge(fivesegs,fivesegs5,how='inner',left_on='nodeidto4',right_on='nodeidfrom')
#        fivesegs=fivesegs[['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                           'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2',
#                           'physicalid3','nodeidfrom3','latfrom3','longfrom3','nodeidto3','latto3','longto3','geodir3',
#                           'physicalid4','nodeidfrom4','latfrom4','longfrom4','nodeidto4','latto4','longto4','geodir4',
#                           'physicalid','nodeidfrom','latfrom','longfrom','nodeidto','latto','longto','geodir']]
#        fivesegs.columns=['physicalid1','nodeidfrom1','latfrom1','longfrom1','nodeidto1','latto1','longto1','geodir1',
#                          'physicalid2','nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2',
#                          'physicalid3','nodeidfrom3','latfrom3','longfrom3','nodeidto3','latto3','longto3','geodir3',
#                          'physicalid4','nodeidfrom4','latfrom4','longfrom4','nodeidto4','latto4','longto4','geodir4',
#                          'physicalid5','nodeidfrom5','latfrom5','longfrom5','nodeidto5','latto5','longto5','geodir5']
#        fivesegs=fivesegs.drop_duplicates(keep='first').reset_index(drop=True)
#        fivesegs['check']=fivesegs[['physicalid1','physicalid2','physicalid3','physicalid4','physicalid5']].apply(lambda x:len(x.unique()),axis=1)
#        fivesegs=fivesegs[fivesegs['check']==5].reset_index(drop=True)
#        if len(fivesegs)>0:
#            fvsg=pd.concat([fvsg[fvsg.index==0]]*len(fivesegs),ignore_index=True)
#            for i in fivesegs.index:
#                fvsg.loc[i,'physicalid1']=fivesegs.loc[i,'physicalid1']
#                fvsg.loc[i,'nodeidfrom1']=fivesegs.loc[i,'nodeidfrom1']
#                fvsg.loc[i,'latfrom1']=fivesegs.loc[i,'latfrom1']
#                fvsg.loc[i,'longfrom1']=fivesegs.loc[i,'longfrom1']
#                fvsg.loc[i,'nodeidto1']=fivesegs.loc[i,'nodeidto1']
#                fvsg.loc[i,'latto1']=fivesegs.loc[i,'latto1']
#                fvsg.loc[i,'longto1']=fivesegs.loc[i,'longto1']
#                fvsg.loc[i,'geodir1']=fivesegs.loc[i,'geodir1']
#                fvsg.loc[i,'physicalid2']=fivesegs.loc[i,'physicalid2']
#                fvsg.loc[i,'nodeidfrom2']=fivesegs.loc[i,'nodeidfrom2']
#                fvsg.loc[i,'latfrom2']=fivesegs.loc[i,'latfrom2']
#                fvsg.loc[i,'longfrom2']=fivesegs.loc[i,'longfrom2']
#                fvsg.loc[i,'nodeidto2']=fivesegs.loc[i,'nodeidto2']
#                fvsg.loc[i,'latto2']=fivesegs.loc[i,'latto2']
#                fvsg.loc[i,'longto2']=fivesegs.loc[i,'longto2']
#                fvsg.loc[i,'geodir2']=fivesegs.loc[i,'geodir2']
#                fvsg.loc[i,'physicalid3']=fivesegs.loc[i,'physicalid3']
#                fvsg.loc[i,'nodeidfrom3']=fivesegs.loc[i,'nodeidfrom3']
#                fvsg.loc[i,'latfrom3']=fivesegs.loc[i,'latfrom3']
#                fvsg.loc[i,'longfrom3']=fivesegs.loc[i,'longfrom3']
#                fvsg.loc[i,'nodeidto3']=fivesegs.loc[i,'nodeidto3']
#                fvsg.loc[i,'latto3']=fivesegs.loc[i,'latto3']
#                fvsg.loc[i,'longto3']=fivesegs.loc[i,'longto3']
#                fvsg.loc[i,'geodir3']=fivesegs.loc[i,'geodir3']
#                fvsg.loc[i,'physicalid4']=fivesegs.loc[i,'physicalid4']
#                fvsg.loc[i,'nodeidfrom4']=fivesegs.loc[i,'nodeidfrom4']
#                fvsg.loc[i,'latfrom4']=fivesegs.loc[i,'latfrom4']
#                fvsg.loc[i,'longfrom4']=fivesegs.loc[i,'longfrom4']
#                fvsg.loc[i,'nodeidto4']=fivesegs.loc[i,'nodeidto4']
#                fvsg.loc[i,'latto4']=fivesegs.loc[i,'latto4']
#                fvsg.loc[i,'longto4']=fivesegs.loc[i,'longto4']
#                fvsg.loc[i,'geodir4']=fivesegs.loc[i,'geodir4']
#                fvsg.loc[i,'physicalid5']=fivesegs.loc[i,'physicalid5']
#                fvsg.loc[i,'nodeidfrom5']=fivesegs.loc[i,'nodeidfrom5']
#                fvsg.loc[i,'latfrom5']=fivesegs.loc[i,'latfrom5']
#                fvsg.loc[i,'longfrom5']=fivesegs.loc[i,'longfrom5']
#                fvsg.loc[i,'nodeidto5']=fivesegs.loc[i,'nodeidto5']
#                fvsg.loc[i,'latto5']=fivesegs.loc[i,'latto5']
#                fvsg.loc[i,'longto5']=fivesegs.loc[i,'longto5']
#                fvsg.loc[i,'geodir5']=fivesegs.loc[i,'geodir5']
#                fvsg.loc[i,'flag']=5
#    return fvsg

## Calculate bearing
#def calcbearing(br):
#    for i in ['1','2','3','4','5']:
#        latfrom=np.radians(br['latfrom'+str(i)])
#        latto=np.radians(br['latto'+str(i)])
#        difflong=np.radians(br['longto'+str(i)]-br['longfrom'+str(i)])
#        bearing=np.degrees(np.arctan2(np.sin(difflong)*np.cos(latfrom),
#                                      np.cos(latfrom)*np.sin(latto)-(np.sin(latfrom)*np.cos(latto)*np.cos(difflong))))
#        br['bearing'+str(i)]=bearing
#    return br

## Calculate street side
#def calcstside(stsd):
#    for i in ['1','2','3','4','5']:
#        if stsd['geodir'+str(i)]=='W':
#            if (stsd['bearing'+str(i)]>=0)&(stsd['bearing'+str(i)]<90):
#                stsd['stside'+str(i)]=np.where(np.isin(stsd['sos'],['E','S']),'R',np.where(np.isin(stsd['sos'],['W','N']),'L',''))
#            elif (stsd['bearing'+str(i)]>=90)&(stsd['bearing'+str(i)]<=180):
#                stsd['stside'+str(i)]=np.where(np.isin(stsd['sos'],['W','S']),'R',np.where(np.isin(stsd['sos'],['E','N']),'L',''))
#            elif (stsd['bearing'+str(i)]<0)&(stsd['bearing'+str(i)]>-90):
#                stsd['stside'+str(i)]=np.where(np.isin(stsd['sos'],['E','N']),'R',np.where(np.isin(stsd['sos'],['W','S']),'L',''))
#            elif (stsd['bearing'+str(i)]<=-90)&(stsd['bearing'+str(i)]>=-180):
#                stsd['stside'+str(i)]=np.where(np.isin(stsd['sos'],['W','N']),'R',np.where(np.isin(stsd['sos'],['E','S']),'L',''))
#        elif stsd['geodir'+str(i)]=='A':
#            if (stsd['bearing'+str(i)]>=0)&(stsd['bearing'+str(i)]<90):
#                stsd['stside'+str(i)]=np.where(np.isin(stsd['sos'],['W','N']),'R',np.where(np.isin(stsd['sos'],['E','S']),'L',''))
#            elif (stsd['bearing'+str(i)]>=90)&(stsd['bearing'+str(i)]<=180):
#                stsd['stside'+str(i)]=np.where(np.isin(stsd['sos'],['E','N']),'R',np.where(np.isin(stsd['sos'],['W','S']),'L',''))
#            elif (stsd['bearing'+str(i)]<0)&(stsd['bearing'+str(i)]>-90):
#                stsd['stside'+str(i)]=np.where(np.isin(stsd['sos'],['W','S']),'R',np.where(np.isin(stsd['sos'],['E','N']),'L',''))
#            elif (stsd['bearing'+str(i)]<=-90)&(stsd['bearing'+str(i)]>=-180):
#                stsd['stside'+str(i)]=np.where(np.isin(stsd['sos'],['E','S']),'R',np.where(np.isin(stsd['sos'],['W','N']),'L',''))
#    return stsd

## Calculate bearing direction
#def calcbrdir(bd):
#    if (bd['bearing']>=0)&(bd['bearing']<90):
#        bd['brdir']='N/E'
#    elif (bd['bearing']>=90)&(bd['bearing']<=180):
#        bd['brdir']='S/E'
#    elif (bd['bearing']<0)&(bd['bearing']>=-90):
#        bd['brdir']='N/W'
#    elif (bd['bearing']<-90)&(bd['bearing']>=-180):
#        bd['brdir']='S/W'
#    return bd

## Reverse geometry
#def reversegeom(rvg):
#    if (rvg['bearingdiff']>90)&(rvg['bearingdiff']<=270):
#        rvg['geom']='LINESTRING ('+', '.join(rvg['geometry'].to_wkt().replace('LINESTRING (','').replace(')','').split(', ')[::-1])+')'
#        if rvg['pvdir']=='N/E':
#            rvg['pvdir']=='S/W'
#        elif rvg['pvdir']=='S/E':
#            rvg['pvdir']=='N/W'
#        elif rvg['pvdir']=='N/W':
#            rvg['pvdir']=='S/E'
#        elif rvg['pvdir']=='S/W':
#            rvg['pvdir']=='N/E'
#    if (rvg['bearingdiff']<=90)|(rvg['bearingdiff']>270):
#        rvg['geom']=rvg['geometry'].to_wkt()
#    return rvg

## Check DOTREG Shapefile geometry
##dsg=dotshpcleanpv[dotshpcleanpv['order']=='P-01276939']
#def checkdotshpgeom(dsg):
#    dsg=dsg.reset_index(drop=True)
#    dsgod=list(dsg['order'])[0]
#    if len(dsg)>=2:
#        latfrom=np.radians(dsg.loc[0,'lat'])
#        latto=np.radians(dsg.loc[len(dsg)-1,'lat'])
#        difflong=np.radians(dsg.loc[len(dsg)-1,'long']-dsg.loc[0,'long'])
#        dsgbearing=np.degrees(np.arctan2(np.sin(difflong)*np.cos(latfrom),
#                                         np.cos(latfrom)*np.sin(latto)-(np.sin(latfrom)*np.cos(latto)*np.cos(difflong))))
#        dsg=dsg.loc[[0]].reset_index(drop=True)
#        dsg.loc[0,'bearing']=dsgbearing
#        dsg.loc[0,'bearingdiff']=abs(dsgbearing-dsg.loc[0,'pvbearing'])
#        if (dsg.loc[0,'bearingdiff']>90)&(dsg.loc[0,'bearingdiff']<=270):
#            dsg.loc[0,'geom']='LINESTRING ('+', '.join(dsg.loc[0,'geometry'].to_wkt().replace('LINESTRING (','').replace(')','').split(', ')[::-1])+')'
#            if dsg.loc[0,'pvdir']=='N/E':
#                dsg.loc[0,'pvdir']=='S/W'
#            elif dsg.loc[0,'pvdir']=='S/E':
#                dsg.loc[0,'pvdir']=='N/W'
#            elif dsg.loc[0,'pvdir']=='N/W':
#                dsg.loc[0,'pvdir']=='S/E'
#            elif dsg.loc[0,'pvdir']=='S/W':
#                dsg.loc[0,'pvdir']=='N/E'
#        elif (dsg.loc[0,'bearingdiff']<=90)|(dsg.loc[0,'bearingdiff']>270):
#            dsg.loc[0,'geom']=dsg.loc[0,'geometry'].to_wkt()
#        dsg=dsg[['order','bearing','bkfaceid','pvbearing','pvdir','shplen','bearingdiff','geom']].reset_index(drop=True)
#    else:
#        print(dsgod+' has only 1 sign')
#        dsg=pd.DataFrame(columns=['order','bearing','bkfaceid','pvbearing','pvdir','shplen','bearingdiff','geom'])
#    return dsg

## Summarize DOTREG SIGN
##ss=dotregsignsum[dotregsignsum['order']=='P-00097673'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='P-00102898'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='S-995465'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='P-00711508'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='P-01255408'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='P-01260824'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='P-01283651'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='P-01284726'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='S-00930087'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='S-127940'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='P-01284517'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='P-01266935'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='P-01289719'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='S-493993'].reset_index(drop=True)
##ss=dotregsignsum[dotregsignsum['order']=='P-01320629']
##ss=dotregsignsum[dotregsignsum['order']=='P-01311835']
##ss=dotregsignsum[dotregsignsum['order']=='P-01328793']
##ss=dotregsignsum[dotregsignsum['order']=='S-344191']
#def sumsign(ss):
#    global taglist
#    try:
#        ssod=list(ss['order'])[0]
#        ss=ss.reset_index(drop=True)
#        if (ss.loc[0,'cl']==1)&(ss.loc[len(ss)-1,'cl']==1)&(len(ss[ss['blpl']==1])==0):
#            ss['dist2']=np.roll(ss['dist'],-1)
#            ss['arrow2']=np.roll(ss['arrow'],-1)
#            ss['cl2']=np.roll(ss['cl'],-1)
#            for i in taglist:
#                ss[i+'2']=0
#            ss=ss.loc[:len(ss)-2].reset_index(drop=True)
#            for i in ss.index[1:]:
#                if ss.loc[i,'arrow']=='':
#                    for j in taglist:
#                        ss.loc[ss['dist']==ss['dist'].unique()[list(ss['dist'].unique()).index(ss.loc[i,'dist'])-1],j+'2']+=ss.loc[i,j]
#                        ss.loc[ss['dist']==ss.loc[i,'dist'],j+'2']+=ss.loc[i,j]
#                elif ss.loc[i,'arrow'][0] in ss.loc[i,'pvdir']:
#                    for j in taglist:
#                        ss.loc[ss['dist']==ss.loc[i,'dist'],j+'2']+=ss.loc[i,j]
#                else:
#                    for j in taglist:
#                        ss.loc[ss['dist']==ss['dist'].unique()[list(ss['dist'].unique()).index(ss.loc[i,'dist'])-1],j+'2']+=ss.loc[i,j]
#            ss=ss.loc[ss['dist']!=ss['dist2'],['order','dist','dist2']+[x+'2' for x in taglist]].reset_index(drop=True)
#            ss.columns=['order','dist1','dist2']+taglist
#            ss=ss[ss['dist1']!=ss['dist2']].sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
#            ss['dist1']=ss['dist1']/ss.loc[len(ss)-1,'dist2']
#            ss['dist2']=ss['dist2']/ss.loc[len(ss)-1,'dist2']
#            return ss
#        elif (ss.loc[0,'cl']==1)&(ss.loc[len(ss)-1,'cl']==1)&(len(ss[ss['blpl']==1])==1):
#            blpl=ss[ss['blpl']==1].reset_index(drop=True)
#            ss=ss[ss['blpl']==0].reset_index(drop=True)
#            ss['dist2']=np.roll(ss['dist'],-1)
#            ss['arrow2']=np.roll(ss['arrow'],-1)
#            ss['cl2']=np.roll(ss['cl'],-1)
#            for i in taglist:
#                ss[i+'2']=0
#            ss=ss.loc[:len(ss)-2].reset_index(drop=True)
#            for i in ss.index[1:]:
#                if ss.loc[i,'arrow']=='':
#                    for j in taglist:
#                        ss.loc[ss['dist']==ss['dist'].unique()[list(ss['dist'].unique()).index(ss.loc[i,'dist'])-1],j+'2']+=ss.loc[i,j]
#                        ss.loc[ss['dist']==ss.loc[i,'dist'],j+'2']+=ss.loc[i,j]
#                elif ss.loc[i,'arrow'][0] in ss.loc[i,'pvdir']:
#                    for j in taglist:
#                        ss.loc[ss['dist']==ss.loc[i,'dist'],j+'2']+=ss.loc[i,j]
#                else:
#                    for j in taglist:
#                        ss.loc[ss['dist']==ss['dist'].unique()[list(ss['dist'].unique()).index(ss.loc[i,'dist'])-1],j+'2']+=ss.loc[i,j]
#            ss=ss.loc[ss['dist']!=ss['dist2'],['order','dist','dist2']+[x+'2' for x in taglist]].reset_index(drop=True)
#            ss.columns=['order','dist1','dist2']+taglist
#            blplpos=ss[(ss['dist1']<=blpl.loc[0,'dist'])&(ss['dist2']>=blpl.loc[0,'dist'])]
#            if blplpos.index[0]==0:
#                blpl1=pd.concat([ss[0:blplpos.index[0]+1],blplpos],axis=0,ignore_index=True)
#                blpl1.loc[len(blpl1)-len(blplpos)-1,'dist2']=blpl.loc[0,'dist']
#                blpl1.loc[len(blpl1)-len(blplpos),'dist1']=blpl.loc[0,'dist']
#                for i in taglist:
#                    blpl1.loc[0:len(blpl1)-len(blplpos)-1,i]=1
#                ss=pd.concat([blpl1,ss[blplpos.index[-1]+1:]],axis=0,ignore_index=True)
#                ss=ss[ss['dist1']!=ss['dist2']].sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
#                ss['dist1']=ss['dist1']/ss.loc[len(ss)-1,'dist2']
#                ss['dist2']=ss['dist2']/ss.loc[len(ss)-1,'dist2']
#                return ss
#            elif blplpos.index[-1]==ss.index[-1]:
#                blpl2=pd.concat([blplpos,ss[blplpos.index[-1]:]],axis=0,ignore_index=True)
#                blpl2.loc[len(blplpos)-1,'dist2']=blpl.loc[0,'dist']
#                blpl2.loc[len(blplpos),'dist1']=blpl.loc[0,'dist']
#                for i in taglist:
#                    blpl2.loc[len(blplpos):,i]=1
#                ss=pd.concat([ss[:blplpos.index[0]],blpl2],axis=0,ignore_index=True)
#                ss=ss[ss['dist1']!=ss['dist2']].sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
#                ss['dist1']=ss['dist1']/ss.loc[len(ss)-1,'dist2']
#                ss['dist2']=ss['dist2']/ss.loc[len(ss)-1,'dist2']
#                return ss
#            else:
#                print(ssod+' clblpl ERROR')
#        elif (ss.loc[0,'cl']==1)&(ss.loc[len(ss)-1,'cl']==1)&(len(ss[ss['blpl']==1])==2):
#            blpl=ss[ss['blpl']==1].reset_index(drop=True)
#            if (list(ss['dist'].unique()).index(list(blpl['dist'])[0])+1)==(list(ss['dist'].unique()).index(list(blpl['dist'])[-1])):
#                ss=ss[ss['blpl']==0].reset_index(drop=True)
#                ss['dist2']=np.roll(ss['dist'],-1)
#                ss['arrow2']=np.roll(ss['arrow'],-1)
#                ss['cl2']=np.roll(ss['cl'],-1)
#                for i in taglist:
#                    ss[i+'2']=0
#                ss=ss.loc[:len(ss)-2].reset_index(drop=True)
#                for i in ss.index[1:]:
#                    if ss.loc[i,'arrow']=='':
#                        for j in taglist:
#                            ss.loc[ss['dist']==ss['dist'].unique()[list(ss['dist'].unique()).index(ss.loc[i,'dist'])-1],j+'2']+=ss.loc[i,j]
#                            ss.loc[ss['dist']==ss.loc[i,'dist'],j+'2']+=ss.loc[i,j]
#                    elif ss.loc[i,'arrow'][0] in ss.loc[i,'pvdir']:
#                        for j in taglist:
#                            ss.loc[ss['dist']==ss.loc[i,'dist'],j+'2']+=ss.loc[i,j]
#                    else:
#                        for j in taglist:
#                            ss.loc[ss['dist']==ss['dist'].unique()[list(ss['dist'].unique()).index(ss.loc[i,'dist'])-1],j+'2']+=ss.loc[i,j]
#                ss=ss.loc[ss['dist']!=ss['dist2'],['order','dist','dist2']+[x+'2' for x in taglist]].reset_index(drop=True)
#                ss.columns=['order','dist1','dist2']+taglist
#                blplpos1=ss[(ss['dist1']<=blpl.loc[0,'dist'])&(ss['dist2']>=blpl.loc[0,'dist'])]
#                blpl1=pd.concat([ss[0:blplpos1.index[0]+1],blplpos1],axis=0,ignore_index=True)
#                blpl1.loc[len(blpl1)-len(blplpos1)-1,'dist2']=blpl.loc[0,'dist']
#                blpl1.loc[len(blpl1)-len(blplpos1),'dist1']=blpl.loc[0,'dist']
#                for i in taglist:
#                    blpl1.loc[0:len(blpl1)-len(blplpos1)-1,i]=1
#                blplpos2=ss[(ss['dist1']<=blpl.loc[1,'dist'])&(ss['dist2']>=blpl.loc[1,'dist'])]
#                blpl2=pd.concat([blpl1.tail(1),ss[blplpos2.index[0]:]],axis=0,ignore_index=True)
#                blpl2.loc[0,'dist2']=blpl.loc[1,'dist']
#                blpl2.loc[1,'dist1']=blpl.loc[1,'dist']
#                for i in taglist:
#                    blpl2.loc[1:,i]=1
#                ss=pd.concat([blpl1[:-1],blpl2],axis=0,ignore_index=True)
#                ss=ss[ss['dist1']!=ss['dist2']].sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
#                ss['dist1']=ss['dist1']/ss.loc[len(ss)-1,'dist2']
#                ss['dist2']=ss['dist2']/ss.loc[len(ss)-1,'dist2']
#                return ss
#            else:
#                ss=ss[ss['blpl']==0].reset_index(drop=True)
#                ss['dist2']=np.roll(ss['dist'],-1)
#                ss['arrow2']=np.roll(ss['arrow'],-1)
#                ss['cl2']=np.roll(ss['cl'],-1)
#                for i in taglist:
#                    ss[i+'2']=0
#                ss=ss.loc[:len(ss)-2].reset_index(drop=True)
#                for i in ss.index[1:]:
#                    if ss.loc[i,'arrow']=='':
#                        for j in taglist:
#                            ss.loc[ss['dist']==ss['dist'].unique()[list(ss['dist'].unique()).index(ss.loc[i,'dist'])-1],j+'2']+=ss.loc[i,j]
#                            ss.loc[ss['dist']==ss.loc[i,'dist'],j+'2']+=ss.loc[i,j]
#                    elif ss.loc[i,'arrow'][0] in ss.loc[i,'pvdir']:
#                        for j in taglist:
#                            ss.loc[ss['dist']==ss.loc[i,'dist'],j+'2']+=ss.loc[i,j]
#                    else:
#                        for j in taglist:
#                            ss.loc[ss['dist']==ss['dist'].unique()[list(ss['dist'].unique()).index(ss.loc[i,'dist'])-1],j+'2']+=ss.loc[i,j]
#                ss=ss.loc[ss['dist']!=ss['dist2'],['order','dist','dist2']+[x+'2' for x in taglist]].reset_index(drop=True)
#                ss.columns=['order','dist1','dist2']+taglist
#                blplpos1=ss[(ss['dist1']<=blpl.loc[0,'dist'])&(ss['dist2']>=blpl.loc[0,'dist'])]
#                blpl1=pd.concat([ss[0:blplpos1.index[0]+1],blplpos1],axis=0,ignore_index=True)
#                blpl1.loc[len(blpl1)-len(blplpos1)-1,'dist2']=blpl.loc[0,'dist']
#                blpl1.loc[len(blpl1)-len(blplpos1),'dist1']=blpl.loc[0,'dist']
#                for i in taglist:
#                    blpl1.loc[0:len(blpl1)-len(blplpos1)-1,i]=1
#                blplpos2=ss[(ss['dist1']<=blpl.loc[1,'dist'])&(ss['dist2']>=blpl.loc[1,'dist'])]
#                blpl2=pd.concat([blplpos2,ss[blplpos2.index[-1]:]],axis=0,ignore_index=True)
#                blpl2.loc[len(blplpos2)-1,'dist2']=blpl.loc[1,'dist']
#                blpl2.loc[len(blplpos2),'dist1']=blpl.loc[1,'dist']
#                for i in taglist:
#                    blpl2.loc[len(blplpos2):,i]=1
#                ss=pd.concat([blpl1,ss[blplpos1.index[-1]+1:blplpos2.index[0]],blpl2],axis=0,ignore_index=True)
#                ss=ss[ss['dist1']!=ss['dist2']].sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
#                ss['dist1']=ss['dist1']/ss.loc[len(ss)-1,'dist2']
#                ss['dist2']=ss['dist2']/ss.loc[len(ss)-1,'dist2']
#                return ss  
#        else:
#            print(ssod+' clblpl ERROR')
#    except:
#        print(ssod+' ERROR')

## Add HYDRANT to SIGNSUM
#def hydrantsign(hs):
#    global dotreglocshppvpk
#    global hydrantadj
#    try:
#        hsod=list(hs['order'])[0]
#        hs=hs.reset_index(drop=True)
#        hs['hdt']=0
#        hsgm=list(dotreglocshppvpk.loc[dotreglocshppvpk['order']==hsod,'geometry'])[0]
#        hdt=hydrantadj[hydrantadj['bkfaceid']==list(dotreglocshppvpk.loc[dotreglocshppvpk['order']==hsod,'bkfaceid'])[0]].reset_index(drop=True)
#        hdt=[hsgm.project(x,normalized=True) for x in hdt['geometry']]
#        for i in hdt:
#            hdtmin=hs[(hs['dist1']<=i-15/hsgm.length)&(hs['dist2']>=i-15/hsgm.length)]
#            hdtmax=hs[(hs['dist1']<=i+15/hsgm.length)&(hs['dist2']>=i+15/hsgm.length)]
#            if (len(hdtmin)==0)&(len(hdtmax)==0):
#                hs['hdt']=1
#            elif len(hdtmin)==0:
#                hdtminmax=pd.concat([hs[:hdtmax.index[0]+1],hdtmax],axis=0,ignore_index=True)
#                hdtminmax.loc[len(hdtminmax)-2,'dist2']=i+15/hsgm.length
#                hdtminmax.loc[len(hdtminmax)-1,'dist1']=i+15/hsgm.length
#                hdtminmax.loc[:len(hdtminmax)-2,'hdt']=1
#                hs=hs[hdtmax.index[0]+1:].reset_index(drop=True)
#                hs=pd.concat([hs,hdtminmax],axis=0,ignore_index=True)
#                hs=hs.sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
#            elif len(hdtmax)==0:
#                hdtminmax=pd.concat([hdtmin,hs[hdtmin.index[0]:]],axis=0,ignore_index=True)
#                hdtminmax.loc[0,'dist2']=i-15/hsgm.length
#                hdtminmax.loc[1,'dist1']=i-15/hsgm.length
#                hdtminmax.loc[1:,'hdt']=1
#                hs=hs[:hdtmin.index[0]].reset_index(drop=True)
#                hs=pd.concat([hs,hdtminmax],axis=0,ignore_index=True)
#                hs=hs.sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
#            elif hdtmax.index-hdtmin.index>=0:
#                hdtminmax=pd.concat([hdtmin,hs[hdtmin.index[0]:hdtmax.index[0]+1],hdtmax],axis=0).reset_index(drop=True)
#                hdtminmax.loc[0,'dist2']=i-15/hsgm.length
#                hdtminmax.loc[1,'dist1']=i-15/hsgm.length
#                hdtminmax.loc[len(hdtminmax)-2,'dist2']=i+15/hsgm.length
#                hdtminmax.loc[len(hdtminmax)-1,'dist1']=i+15/hsgm.length
#                hdtminmax.loc[1:len(hdtminmax)-2,'hdt']=1
#                hs=pd.concat([hs[:hdtmin.index[0]],hs[hdtmax.index[0]+1:]],axis=0,ignore_index=True)
#                hs=pd.concat([hs,hdtminmax],axis=0,ignore_index=True)
#                hs=hs.sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
#        return hs
#    except:
#        print(hsod+' ERROR')

## Split blockface geometry
##sg=dotregsignsumhdt[dotregsignsumhdt['order']=='P-01290459']
##sg=dotregsignsumhdt[dotregsignsumhdt['order']=='P-01311167']
##sg=dotregsignsumhdt[dotregsignsumhdt['order']=='P-01313775']
##sg=dotregsignsumhdt[dotregsignsumhdt['order']=='S-00313407']
##sg=dotregsignsumhdt[dotregsignsumhdt['order']=='S-343270']
##sg=dotregsignsumhdt[dotregsignsumhdt['order']=='S-994259']
#def splitgm(sg):
#    global dotreglocshppvpk
#    try:
#        sgod=list(sg['order'])[0]
#        sg=sg.reset_index(drop=True)
#        sggm=list(dotreglocshppvpk.loc[dotreglocshppvpk['order']==sgod,'geometry'])[0]
#        splitpos=[x for x in list(sg['dist1'])+[list(sg['dist2'])[-1]]]
#        splitter=shapely.geometry.MultiPoint([sggm.interpolate(x,normalized=True) for x in splitpos])
#        shapesplit=shapely.ops.split(sggm,splitter.buffer(1e-8))
#        shapesplit=[shapesplit[x].wkt for x in range(1,len(shapesplit),2)]
#        sg['geom']=shapesplit
#        return sg
#    except:
#        print(sgod+' ERROR')
#
## Add HYDRANT to LION parkable blockface without DOTREG SIGN
#def lionpkhydrant(lh):
#    global pvmtedgemdn
#    global hydrantadj
#    try:
#        lhbk=list(lh['bkfaceid'])[0]
#        lh=lh.reset_index(drop=True)
#        lhgm=list(pvmtedgemdn.loc[pvmtedgemdn['bkfaceid']==lhbk,'geometry'])[0]
#        lh=pd.concat([lh,lh,lh],axis=0,ignore_index=True)
#        lh.loc[0,'dist1']=0
#        lh.loc[0,'dist2']=15/lhgm.length
#        lh.loc[1,'dist1']=15/lhgm.length
#        lh.loc[1,'dist2']=(lhgm.length-15)/lhgm.length 
#        lh.loc[2,'dist1']=(lhgm.length-15)/lhgm.length
#        lh.loc[2,'dist2']=1
#        lhdt=hydrantadj[hydrantadj['bkfaceid']==lhbk].reset_index(drop=True)
#        lhdt=[lhgm.project(x,normalized=True) for x in lhdt['geometry']]
#        for i in lhdt:
#            lhdtmin=lh[(lh['dist1']<=i-15/lhgm.length)&(lh['dist2']>=i-15/lhgm.length)]
#            lhdtmax=lh[(lh['dist1']<=i+15/lhgm.length)&(lh['dist2']>=i+15/lhgm.length)]
#            if (len(lhdtmin)==0)&(len(lhdtmax)==0):
#                lh['hdt']=1
#            elif len(lhdtmin)==0:
#                lhdtminmax=pd.concat([lh[:lhdtmax.index[0]+1],lhdtmax],axis=0,ignore_index=True)
#                lhdtminmax.loc[len(lhdtminmax)-2,'dist2']=i+15/lhgm.length
#                lhdtminmax.loc[len(lhdtminmax)-1,'dist1']=i+15/lhgm.length
#                lhdtminmax.loc[:len(lhdtminmax)-2,'hdt']=1
#                lh=lh[lhdtmax.index[0]+1:].reset_index(drop=True)
#                lh=pd.concat([lh,lhdtminmax],axis=0,ignore_index=True)
#                lh=lh.sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
#            elif len(lhdtmax)==0:
#                lhdtminmax=pd.concat([lhdtmin,lh[lhdtmin.index[0]:]],axis=0,ignore_index=True)
#                lhdtminmax.loc[0,'dist2']=i-15/lhgm.length
#                lhdtminmax.loc[1,'dist1']=i-15/lhgm.length
#                lhdtminmax.loc[1:,'hdt']=1
#                lh=lh[:lhdtmin.index[0]].reset_index(drop=True)
#                lh=pd.concat([lh,lhdtminmax],axis=0,ignore_index=True)
#                lh=lh.sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
#            elif lhdtmax.index-lhdtmin.index>=0:
#                lhdtminmax=pd.concat([lhdtmin,lh[lhdtmin.index[0]:lhdtmax.index[0]+1],lhdtmax],axis=0).reset_index(drop=True)
#                lhdtminmax.loc[0,'dist2']=i-15/lhgm.length
#                lhdtminmax.loc[1,'dist1']=i-15/lhgm.length
#                lhdtminmax.loc[len(lhdtminmax)-2,'dist2']=i+15/lhgm.length
#                lhdtminmax.loc[len(lhdtminmax)-1,'dist1']=i+15/lhgm.length
#                lhdtminmax.loc[1:len(lhdtminmax)-2,'hdt']=1
#                lh=pd.concat([lh[:lhdtmin.index[0]],lh[lhdtmax.index[0]+1:]],axis=0,ignore_index=True)
#                lh=pd.concat([lh,lhdtminmax],axis=0,ignore_index=True)
#                lh=lh.sort_values(['dist1','dist2'],ascending=True).reset_index(drop=True)
#        return lh
#    except:
#        print(str(lhbk)+' ERROR')

## Split LION parkable blockface without DOTREG SIGN geometry
#def lionpksplitgeom(lsg):
#    global pvmtedgemdn
#    try:
#        lsgbk=list(lsg['bkfaceid'])[0]
#        lsg=lsg.reset_index(drop=True)
#        lsggm=list(pvmtedgemdn.loc[pvmtedgemdn['bkfaceid']==lsgbk,'geometry'])[0]
#        lsplitpos=[x for x in list(lsg['dist1'])+[list(lsg['dist2'])[-1]]]
#        lsplitter=shapely.geometry.MultiPoint([lsggm.interpolate(x,normalized=True) for x in lsplitpos])
#        lshapesplit=shapely.ops.split(lsggm,lsplitter.buffer(1e-8))
#        lshapesplit=[lshapesplit[x].wkt for x in range(1,len(lshapesplit),2)]
#        lsg['geom']=lshapesplit
#        return lsg
#    except:
#        print(str(lsgbk)+' ERROR')



























































## Clean PAVEMENTEDGE and MEDIAN (8 mins)
#start=datetime.datetime.now()
#pvmtedge=gpd.read_file(path+'PLANIMETRIC/PAVEMENTEDGE.shp')
#pvmtedge=pvmtedge.to_crs({'init':'epsg:4326'})
#pvmtedge['bkfaceid']=pd.to_numeric(pvmtedge['BLOCKFACEI'])
#pvmtedge['shplen']=pd.to_numeric(pvmtedge['SHAPE_Leng'])
#pvmtedge=pvmtedge[['bkfaceid','shplen','geometry']].reset_index(drop=True)
#mdn=gpd.read_file(path+'PLANIMETRIC/MEDIAN.shp')
#mdn=mdn.to_crs({'init':'epsg:4326'})
#mdn['medians']=1
#mdn=mdn[['medians','geometry']].reset_index(drop=True)
#pvmtedgemdn=gpd.sjoin(pvmtedge,mdn,how='left',op='intersects')
#pvmtedgemdn['medians']=np.where(pd.isna(pvmtedgemdn['medians']),0,pvmtedgemdn['medians'])
#pvmtedgemdn=pvmtedgemdn.groupby(['bkfaceid','shplen'],as_index=False).agg({'medians':max}).reset_index(drop=True)
#pvmtedgemdn=pd.merge(pvmtedge,pvmtedgemdn,how='left',on=['bkfaceid','shplen'])
#pvmtedgemdn=pvmtedgemdn[[type(x)==shapely.geometry.linestring.LineString for x in pvmtedgemdn['geometry']]]
#pvmtedgemdn['latfrom']=[x.xy[1][0] for x in pvmtedgemdn['geometry']]
#pvmtedgemdn['longfrom']=[x.xy[0][0] for x in pvmtedgemdn['geometry']]
#pvmtedgemdn['latto']=[x.xy[1][-1] for x in pvmtedgemdn['geometry']]
#pvmtedgemdn['longto']=[x.xy[0][-1] for x in pvmtedgemdn['geometry']]
#pvmtedgemdn['pvbearing']=np.nan
#pvmtedgemdn['pvdir']=''
#pvmtedgemdn=pvmtedgemdn.apply(calcpvmtbearing,axis=1)
#pvmtedgemdn=pvmtedgemdn.apply(calcpvmtdir,axis=1)
#pvmtedgemdn=pvmtedgemdn[['bkfaceid','pvbearing','pvdir','shplen','medians','geometry']].reset_index(drop=True)
#pvmtedgemdn.crs={'init':'epsg:4326'}
#pvmtedgemdn.to_file(path+'PLANIMETRIC/PVMTEDGEMDN.shp')
#print(datetime.datetime.now()-start)

## Clean HYDRANT (90 mins)
#start=datetime.datetime.now()
#hydrant=gpd.read_file(path+'HYDRANT/HYDRANT.shp')
#hydrant.crs={'init':'epsg:4326'}
#hydrant=hydrant.to_crs({'init':'epsg:6539'})
#hydrantadj=hydrant[['unitid','geometry']].reset_index(drop=True)
#hydrantadj['orggeom']=[x.wkt for x in hydrantadj['geometry']]
#hydrantadj['bkfaceid']=np.nan
#hydrantadj['snapdist']=np.nan
#hydrantadj['adjgeom']=''
#pvmtedgemdn=gpd.read_file(path+'PLANIMETRIC/PVMTEDGEMDN.shp')
#pvmtedgemdn.crs={'init':'epsg:4326'}
#pvmtedgemdn=pvmtedgemdn.to_crs({'init':'epsg:6539'})
#hydrantbuffer=gpd.GeoDataFrame(hydrantadj[['unitid']],geometry=hydrantadj['geometry'].buffer(50),crs={'init':'epsg:6539'}).reset_index(drop=True)
#hydrantbuffer=gpd.sjoin(hydrantbuffer,pvmtedgemdn,how='left',op='intersects')
#hydrantbuffer=hydrantbuffer[['unitid','bkfaceid']].dropna(axis=0,how='any').drop_duplicates(keep='first').reset_index(drop=True)
#hydrantadj=hydrantadj.apply(adjhydrant,axis=1)
#hydrantadj=hydrantadj.drop(['geometry'],axis=1).dropna(axis=0,how='any').drop_duplicates(keep='first').reset_index(drop=True)
#hydrantadj=gpd.GeoDataFrame(hydrantadj,geometry=hydrantadj['adjgeom'].map(wkt.loads),crs={'init':'epsg:6539'})
#hydrantadj=hydrantadj.to_crs({'init':'epsg:4326'})
#hydrantadj.to_file(path+'HYDRANT/HYDRANTADJ.shp')
#print(datetime.datetime.now()-start)

## Clean DOTREG SIGN (2.5 mins)
#start=datetime.datetime.now()
#dotregsign=pd.read_csv(path+'DOTREG/SIGN.csv',encoding='latin_1',dtype=str)
#dotregsignclean=dotregsign.copy()
#dotregsignclean['boro']=[' '.join(x.split()).upper() for x in dotregsignclean['SRP_Boro']]
#dotregsignclean['boro']=np.where(dotregsignclean['boro']=='M',1,np.where(dotregsignclean['boro']=='B',2,np.where(dotregsignclean['boro']=='K',3,np.where(dotregsignclean['boro']=='Q',4,np.where(dotregsignclean['boro']=='S',5,0)))))
#dotregsignclean['order']=[' '.join(x.split()).upper() for x in dotregsignclean['SRP_Order']]
#dotregsignclean['seq']=pd.to_numeric(dotregsignclean['SRP_Seq'])
#dotregsignclean['dist']=pd.to_numeric(dotregsignclean['SR_Distx'])
#dotregsignclean['arrow']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in dotregsignclean['SR_Arrow']]
#dotregsignclean['desc']=''
#dotregsignclean['mutcd']=''
#dotregsignclean=dotregsignclean.apply(sepdescmutcd,axis=1)
#dotregsignclean=dotregsignclean[['boro','order','seq','dist','arrow','desc','mutcd']].drop_duplicates(keep='first').reset_index(drop=True)
#dotregsignclean.to_csv(path+'DOTREG/SIGNCLEAN.csv',index=False)
#print(datetime.datetime.now()-start)

## Clean DOTREG SIGN MUTCD (8 mins)
#start=datetime.datetime.now()
#dotregsignclean=pd.read_csv(path+'DOTREG/SIGNCLEAN.csv',dtype=str,converters={'boro':float,'seq':float,'dist':float})
#mutcd=dotregsignclean.groupby(['mutcd','desc'],as_index=False).agg({'order':'count'}).rename(columns={'order':'count'}).reset_index(drop=True)
#mutcd['mutcd']=[' '.join(x.split()).upper() for x in mutcd['mutcd']]
#mutcd['desc']=[' '.join(x.split()).upper() for x in mutcd['desc']]
#mutcd=mutcd.drop_duplicates(keep='first').sort_values('count',ascending=False).reset_index(drop=True)
#mutcd['descnum']=[len(list(re.finditer('>',x))) for x in mutcd['desc']]
#mutcd['desc1']=''
#mutcd['dir1']=0
#mutcd['desc2']=''
#mutcd['dir2']=0
#mutcd['desc3']=''
#mutcd['dir3']=0
#mutcd['desctail']=''
#mutcd=mutcd.apply(sepdesc,axis=1)
#mutcd1=mutcd[['mutcd','descnum','desc','desc1','dir1']].rename(columns={'desc':'orgdesc','desc1':'sepdesc','dir1':'dir'})
#mutcd1['descseq']=1
#mutcd2=mutcd[['mutcd','descnum','desc','desc2','dir2']].rename(columns={'desc':'orgdesc','desc2':'sepdesc','dir2':'dir'})
#mutcd2['descseq']=2
#mutcd3=mutcd[['mutcd','descnum','desc','desc3','dir3']].rename(columns={'desc':'orgdesc','desc3':'sepdesc','dir3':'dir'})
#mutcd3['descseq']=3
#mutcd=pd.concat([mutcd1,mutcd2,mutcd3],axis=0).dropna(axis=0,how='any').reset_index(drop=True)
#mutcd=mutcd.loc[mutcd['dir']!=0,['mutcd','orgdesc','descnum','descseq','sepdesc','dir']].reset_index(drop=True)
#mutcd=mutcd[~np.isin(mutcd['mutcd'],[x+'A' for x in mutcd['mutcd']])].reset_index(drop=True)
#mutcd['type']=''
#mutcd['time']=''
#mutcd['m']=0
#mutcd['t']=0
#mutcd['w']=0
#mutcd['r']=0
#mutcd['f']=0
#mutcd['s']=0
#mutcd['u']=0
#mutcd['daysflag']=0
#mutcd['dayscheck']=0
#mutcd['starthour']=''
#mutcd['endhour']=''
#mutcd['hoursflag']=0
#mutcd['hourscheck']=0
#mutcd['manual']=0
#for i in mutcd.index:
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('DESCRIPTION NOT AVAILABLE',mutcd.loc[i,'sepdesc'])))>0,'DNA',mutcd.loc[i,'type'])
#    if mutcd.loc[i,'type']=='DNA':
#        mutcd.loc[i,['m','t','w','r','f','s','u']]=0
#        mutcd.loc[i,['starthour','endhour']]=''
#        mutcd.loc[i,['daysflag','hoursflag']]+=1
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('HOUR PARKING',mutcd.loc[i,'sepdesc'])))>0,'HOUR PARKING',mutcd.loc[i,'type'])
#    if mutcd.loc[i,'type']=='HOUR PARKING':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('HOUR PARKING',mutcd.loc[i,'sepdesc']))[0].start()+12:].split())
#        mutcd.loc[i,['m','t','w','r','f','s','u']]=0
#        mutcd.loc[i,['starthour','endhour']]=''
#        mutcd.loc[i,['daysflag','hoursflag']]+=1
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('HMP',mutcd.loc[i,'sepdesc'])))>0,'HMP',mutcd.loc[i,'type'])
#    if mutcd.loc[i,'type']=='HMP':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('HMP',mutcd.loc[i,'sepdesc']))[0].start()+3:].split())
#        mutcd.loc[i,['m','t','w','r','f','s','u']]=0
#        mutcd.loc[i,['starthour','endhour']]=''
#        mutcd.loc[i,['daysflag','hoursflag']]+=1
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('HOUR METERED PARKING',mutcd.loc[i,'sepdesc'])))>0,'HOUR METERED PARKING',mutcd.loc[i,'type'])
#    if mutcd.loc[i,'type']=='HOUR METERED PARKING':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('HOUR METERED PARKING',mutcd.loc[i,'sepdesc']))[0].start()+20:].split())
#        mutcd.loc[i,['m','t','w','r','f','s','u']]=0
#        mutcd.loc[i,['starthour','endhour']]=''
#        mutcd.loc[i,['daysflag','hoursflag']]+=1
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('COMMERCIAL VEHICLES',mutcd.loc[i,'sepdesc'])))>0,'COMMERCIAL',mutcd.loc[i,'type'])
#    if mutcd.loc[i,'type']=='COMMERCIAL':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('COMMERCIAL VEHICLES',mutcd.loc[i,'sepdesc']))[0].start()+19:].split())
#        mutcd.loc[i]=extractdays(mutcd.loc[i])
#        mutcd.loc[i]=extracthours(mutcd.loc[i])
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('TRUCK \(SYMBOL\)',mutcd.loc[i,'sepdesc'])))>0,'TRUCK',mutcd.loc[i,'type'])
#    if mutcd.loc[i,'type']=='TRUCK':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('TRUCK \(SYMBOL\)',mutcd.loc[i,'sepdesc']))[0].start()+14:].split())
#        mutcd.loc[i]=extractdays(mutcd.loc[i])
#        mutcd.loc[i]=extracthours(mutcd.loc[i])
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('TAXI \(SYMBOL\)',mutcd.loc[i,'sepdesc'])))>0,'TAXI',mutcd.loc[i,'type'])
#    if mutcd.loc[i,'type']=='TAXI':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('TAXI \(SYMBOL\)',mutcd.loc[i,'sepdesc']))[0].start()+13:].split())
#        mutcd.loc[i]=extractdays(mutcd.loc[i])
#        mutcd.loc[i]=extracthours(mutcd.loc[i])
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('TAXI HAIL \(SYMBOL\)',mutcd.loc[i,'sepdesc'])))>0,'TAXI HAIL',mutcd.loc[i,'type'])
#    if mutcd.loc[i,'type']=='TAXI HAIL':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('TAXI HAIL \(SYMBOL\)',mutcd.loc[i,'sepdesc']))[0].start()+18:].split())
#        mutcd.loc[i]=extractdays(mutcd.loc[i])
#        mutcd.loc[i]=extracthours(mutcd.loc[i])
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('TAXI HAILING \(SYMBOL\)',mutcd.loc[i,'sepdesc'])))>0,'TAXI HAILING',mutcd.loc[i,'type'])
#    if mutcd.loc[i,'type']=='TAXI HAILING':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('TAXI HAILING \(SYMBOL\)',mutcd.loc[i,'sepdesc']))[0].start()+21:].split())
#        mutcd.loc[i]=extractdays(mutcd.loc[i])
#        mutcd.loc[i]=extracthours(mutcd.loc[i])
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('FHV \(SYMBOL\)',mutcd.loc[i,'sepdesc'])))>0,'FHV',mutcd.loc[i,'type'])
#    if mutcd.loc[i,'type']=='FHV':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('FHV \(SYMBOL\)',mutcd.loc[i,'sepdesc']))[0].start()+12:].split())
#        mutcd.loc[i]=extractdays(mutcd.loc[i])
#        mutcd.loc[i]=extracthours(mutcd.loc[i])
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('STAR \(SYMBOL\)',mutcd.loc[i,'sepdesc'])))>0,'STAR',mutcd.loc[i,'type'])    
#    if mutcd.loc[i,'type']=='STAR':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('STAR \(SYMBOL\)',mutcd.loc[i,'sepdesc']))[0].start()+13:].split())
#        mutcd.loc[i]=extractdays(mutcd.loc[i])
#        mutcd.loc[i]=extracthours(mutcd.loc[i])
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('CROSS \(SYMBOL\)',mutcd.loc[i,'sepdesc'])))>0,'CROSS',mutcd.loc[i,'type'])    
#    if mutcd.loc[i,'type']=='CROSS':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('CROSS \(SYMBOL\)',mutcd.loc[i,'sepdesc']))[0].start()+14:].split())
#        mutcd.loc[i]=extractdays(mutcd.loc[i])
#        mutcd.loc[i]=extracthours(mutcd.loc[i])
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('BUS \(SYMBOL\)',mutcd.loc[i,'sepdesc'])))>0,'BUS',mutcd.loc[i,'type'])    
#    if mutcd.loc[i,'type']=='BUS':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('BUS \(SYMBOL\)',mutcd.loc[i,'sepdesc']))[0].start()+12:].split())
#        mutcd.loc[i]=extractdays(mutcd.loc[i])
#        mutcd.loc[i]=extracthours(mutcd.loc[i])
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('NO STOPPING',mutcd.loc[i,'sepdesc'])))>0,'NO STOPPING',mutcd.loc[i,'type'])
#    if mutcd.loc[i,'type']=='NO STOPPING':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('NO STOPPING',mutcd.loc[i,'sepdesc']))[0].start()+11:].split())
#        mutcd.loc[i]=extractdays(mutcd.loc[i])
#        mutcd.loc[i]=extracthours(mutcd.loc[i])
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('NO STANDING',mutcd.loc[i,'sepdesc'])))>0,'NO STANDING',mutcd.loc[i,'type'])
#    if mutcd.loc[i,'type']=='NO STANDING':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('NO STANDING',mutcd.loc[i,'sepdesc']))[0].start()+11:].split())
#        mutcd.loc[i]=extractdays(mutcd.loc[i])
#        mutcd.loc[i]=extracthours(mutcd.loc[i])
#    mutcd.loc[i,'type']=np.where(len(list(re.finditer('NO PARKING',mutcd.loc[i,'sepdesc'])))>0,'NO PARKING',mutcd.loc[i,'type'])
#    if mutcd.loc[i,'type']=='NO PARKING':
#        mutcd.loc[i,'time']=' '.join(mutcd.loc[i,'sepdesc'][list(re.finditer('NO PARKING',mutcd.loc[i,'sepdesc']))[0].start()+10:].split())
#        mutcd.loc[i]=extractdays(mutcd.loc[i])
#        mutcd.loc[i]=extracthours(mutcd.loc[i])
#    mutcd.to_csv(path+'DOTREG/MUTCD.csv',index=False)
#print(datetime.datetime.now()-start)

# Manually check and categorize MUTCD

## Create No Parking Tags for MUTCDCHECK (2 mins)
#start=datetime.datetime.now()
#mutcdcheck=pd.read_csv(path+'DOTREG/MUTCDCHECK.csv',dtype=float,converters={'mutcd':str,'orgdesc':str,'sepdesc':str,'type':str,
#                                                                            'time':str,'starthour':str,'endhour':str})
#mutcdtag=mutcdcheck.copy()
#for i in [str(x)+str(y) for x in ['m','t','w','r','f','s','u'] for y in [str(x)[11:13]+str(x)[14:16] for x in pd.date_range('00:00','23:30',freq='30min')]]:
#    mutcdtag[i]=0
#for i in mutcdtag.index:
#    if (mutcdtag.loc[i,'dayscheck']==1)&(mutcdtag.loc[i,'hourscheck']==1):
#        if pd.Timestamp(mutcdtag.loc[i,'starthour'])<pd.Timestamp(mutcdtag.loc[i,'endhour']):
#            npdays=[x for x in ['m','t','w','r','f','s','u'] if mutcdtag.loc[i,x]>0]
#            nphours=[str(x)[11:13]+str(x)[14:16] for x in pd.date_range(mutcdtag.loc[i,'starthour'],mutcdtag.loc[i,'endhour'],freq='30min')][:-1]
#            npdayshours=[str(x)+str(y) for x in npdays for y in nphours]
#            mutcdtag.loc[i,npdayshours]=1
#        elif pd.Timestamp(mutcdtag.loc[i,'starthour'])>=pd.Timestamp(mutcdtag.loc[i,'endhour']):
#            npdays1=[x for x in ['m','t','w','r','f','s','u'] if mutcdtag.loc[i,x]>0]
#            nphours1=[str(x)[11:13]+str(x)[14:16] for x in pd.date_range(mutcdtag.loc[i,'starthour'],'23:30',freq='30min')]
#            npdayshours1=[str(x)+str(y) for x in npdays1 for y in nphours1]
#            npdays2=[['m','t','w','r','f','s','u','m'][list(['m','t','w','r','f','s','u']).index(x)+1] for x in ['m','t','w','r','f','s','u'] if mutcdtag.loc[i,x]>0]
#            nphours2=[str(x)[11:13]+str(x)[14:16] for x in pd.date_range('00:00',mutcdtag.loc[i,'endhour'],freq='30min')][:-1]
#            npdayshours2=[str(x)+str(y) for x in npdays2 for y in nphours2]
#            mutcdtag.loc[i,npdayshours1+npdayshours2]=1
#mutcdtag.to_csv(path+'DOTREG/MUTCDTAG.csv',index=False)
#print(datetime.datetime.now()-start)

# Manually check and categorize MUTCDTAG

## Create Metered Parking Tags for MUTCD (1 min)
## Manually filter potential metered parking
#start=datetime.datetime.now()
#mutcdcheck=pd.read_csv(path+'DOTREG/MUTCDCHECK.csv',dtype=float,converters={'mutcd':str,'orgdesc':str,'sepdesc':str,'type':str,
#                                                                            'time':str,'starthour':str,'endhour':str})
#mutcdhmp=mutcdcheck[np.isin(mutcdcheck['type'],['HMP','HOUR METERED PARKING',''])].reset_index(drop=True)
#for i in mutcdhmp.index:
#    mutcdhmp.loc[i]=extractdays(mutcdhmp.loc[i])
#    mutcdhmp.loc[i]=extracthours(mutcdhmp.loc[i])
#mutcdhmp.to_csv(path+'DOTREG/MUTCDHMP.csv',index=False)
#print(datetime.datetime.now()-start)

# Manually check and categorize MUTCDHMP

## Create Metered Parking Tags for MUTCDCHECK (1 min)
#start=datetime.datetime.now()
#mutcdhmpcheck=pd.read_csv(path+'DOTREG/MUTCDHMPCHECK.csv',dtype=float,converters={'mutcd':str,'orgdesc':str,'sepdesc':str,'type':str,
#                                                                                  'time':str,'starthour':str,'endhour':str})
#mutcdhmptag=mutcdhmpcheck.copy()
#for i in [str(x)+str(y) for x in ['m','t','w','r','f','s','u'] for y in [str(x)[11:13]+str(x)[14:16] for x in pd.date_range('00:00','23:30',freq='30min')]]:
#    mutcdhmptag[i]=0
#for i in mutcdhmptag.index:
#    if (mutcdhmptag.loc[i,'dayscheck']==1)&(mutcdhmptag.loc[i,'hourscheck']==1):
#        if pd.Timestamp(mutcdhmptag.loc[i,'starthour'])<pd.Timestamp(mutcdhmptag.loc[i,'endhour']):
#            npdays=[x for x in ['m','t','w','r','f','s','u'] if mutcdhmptag.loc[i,x]>0]
#            nphours=[str(x)[11:13]+str(x)[14:16] for x in pd.date_range(mutcdhmptag.loc[i,'starthour'],mutcdhmptag.loc[i,'endhour'],freq='30min')][:-1]
#            npdayshours=[str(x)+str(y) for x in npdays for y in nphours]
#            mutcdhmptag.loc[i,npdayshours]=1
#        elif pd.Timestamp(mutcdhmptag.loc[i,'starthour'])>=pd.Timestamp(mutcdhmptag.loc[i,'endhour']):
#            npdays1=[x for x in ['m','t','w','r','f','s','u'] if mutcdhmptag.loc[i,x]>0]
#            nphours1=[str(x)[11:13]+str(x)[14:16] for x in pd.date_range(mutcdhmptag.loc[i,'starthour'],'23:30',freq='30min')]
#            npdayshours1=[str(x)+str(y) for x in npdays1 for y in nphours1]
#            npdays2=[['m','t','w','r','f','s','u','m'][list(['m','t','w','r','f','s','u']).index(x)+1] for x in ['m','t','w','r','f','s','u'] if mutcdhmptag.loc[i,x]>0]
#            nphours2=[str(x)[11:13]+str(x)[14:16] for x in pd.date_range('00:00',mutcdhmptag.loc[i,'endhour'],freq='30min')][:-1]
#            npdayshours2=[str(x)+str(y) for x in npdays2 for y in nphours2]
#            mutcdhmptag.loc[i,npdayshours1+npdayshours2]=1
#mutcdhmptag.to_csv(path+'DOTREG/MUTCDHMPTAG.csv',index=False)
#print(datetime.datetime.now()-start)

# Manually check and categorize MUTCDHMPTAG

## Filter LION blockface and calculate blockface parkability (1 min)
#start=datetime.datetime.now()
#lion=gpd.read_file(path+'LION/LION.shp')
#lionbfpk=lion[['LBlockFace','RBlockFace','FeatureTyp','SegmentTyp','RW_TYPE','TrafDir','Number_Par']].drop_duplicates().reset_index(drop=True)
#lionbfpk['lbkfaceid']=pd.to_numeric(lionbfpk['LBlockFace'])
#lionbfpk['rbkfaceid']=pd.to_numeric(lionbfpk['RBlockFace'])
#lionbfpk['featuretype']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionbfpk['FeatureTyp']]
#lionbfpk=lionbfpk[np.isin(lionbfpk['featuretype'],['0','6','A','C'])].reset_index(drop=True)
#lionbfpk['segmenttype']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionbfpk['SegmentTyp']]
#lionbfpk=lionbfpk[np.isin(lionbfpk['segmenttype'],['B','R','U','S'])].reset_index(drop=True)
#lionbfpk['rwtype']=pd.to_numeric(lionbfpk['RW_TYPE'])
#lionbfpk=lionbfpk[np.isin(lionbfpk['rwtype'],[1])].reset_index(drop=True)
#lionbfpk['trafficdir']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in lionbfpk['TrafDir']]
#lionbfpk=lionbfpk[np.isin(lionbfpk['trafficdir'],['T','W','A'])].reset_index(drop=True)
#lionbfpk['parkinglane']=pd.to_numeric(lionbfpk['Number_Par']).fillna(0)
#lionbfpk=lionbfpk[['lbkfaceid','rbkfaceid','trafficdir','parkinglane']].drop_duplicates(keep='first').reset_index(drop=True)
#lionbfpk['lparking']=np.nan
#lionbfpk['rparking']=np.nan
#lionbfpk=lionbfpk.apply(lionpkbkface,axis=1)
#lionbfpkl=lionbfpk[['lbkfaceid','lparking']].rename(columns={'lbkfaceid':'bkfaceid','lparking':'parking'})
#lionbfpkr=lionbfpk[['rbkfaceid','rparking']].rename(columns={'rbkfaceid':'bkfaceid','rparking':'parking'})
#lionbfpk=pd.concat([lionbfpkl,lionbfpkr],axis=0,ignore_index=True)
#lionbfpk=lionbfpk.dropna(axis=0)
#lionbfpk=lionbfpk.drop_duplicates(keep='first').sort_values(['bkfaceid','parking'],ascending=True).reset_index(drop=True)
#lionbfpk['parking']=[str(x) for x in lionbfpk['parking']]
#lionbfpk=lionbfpk.groupby('bkfaceid',as_index=False).agg('/'.join).reset_index(drop=True)
## Still need to solve the 0.0/0.1/0.5/0.9 parking!!!
#lionbfpk.to_csv(path+'LION/LIONBFPK.csv',index=False)
#print(datetime.datetime.now()-start)

## Streetname-Streetcode from SND (0.1 min)
#start=datetime.datetime.now()
#snd=pd.read_csv(path+'SND/snd19Bcow.txt',dtype=str,skiprows=1,header=None)
#snd['boro']=pd.to_numeric([x[1:2] for x in snd[0]])
#snd['streetname']=[' '.join(x[2:34].split()).upper() for x in snd[0]]
#snd['streetcode']=pd.to_numeric([' '.join(x[36:42].split()).upper() for x in snd[0]])
#snd=snd[['boro','streetname','streetcode']].dropna(axis=0,how='any').drop_duplicates(keep='first').reset_index(drop=True)
#snd.to_csv(path+'SND/SND.csv',index=False)
#print(datetime.datetime.now()-start)

## Nodeid-Streetcode from LION (1 min)
#start=datetime.datetime.now()
#lion=gpd.read_file(path+'LION/LION.shp')
## Find Dead End Nodes
#lionstrb=lion[np.isin(lion['FeatureTyp'],['0','6','A','C','W'])].reset_index(drop=True)
#lionstrb=lionstrb[np.isin(lionstrb['SegmentTyp'],['B','R','E','T','C','S','U'])].reset_index(drop=True)
#lionstrb=lionstrb.drop_duplicates('SegmentID',keep='first')
#fromnode=lionstrb[['NodeIDFrom','XFrom','YFrom','StreetCode']].reset_index(drop=True)
#fromnode['nodeid']=pd.to_numeric(fromnode['NodeIDFrom'])
#fromnode['x']=pd.to_numeric(fromnode['XFrom'])
#fromnode['y']=pd.to_numeric(fromnode['YFrom'])
#fromnode['streetcode']=pd.to_numeric(fromnode['StreetCode'])
#fromnode=fromnode[['nodeid','x','y','streetcode']].reset_index(drop=True)
#tonode=lionstrb[['NodeIDTo','XTo','YTo','StreetCode']].reset_index(drop=True)
#tonode['nodeid']=pd.to_numeric(tonode['NodeIDTo'])
#tonode['x']=pd.to_numeric(tonode['XTo'])
#tonode['y']=pd.to_numeric(tonode['YTo'])
#tonode['streetcode']=pd.to_numeric(tonode['StreetCode'])
#tonode=tonode[['nodeid','x','y','streetcode']].reset_index(drop=True)
#node=pd.concat([fromnode,tonode],axis=0,ignore_index=True)
#deadend=node.groupby('nodeid',as_index=False).agg({'x':'mean','y':'mean','streetcode':'count'}).reset_index(drop=True)
#deadend['deadend']=np.where(deadend['streetcode']==1,1,0)
#deadend=deadend[deadend['deadend']==1].reset_index(drop=True)
#deadend=gpd.GeoDataFrame(deadend,geometry=[shapely.geometry.Point(x,y) for x,y in zip(deadend['x'],deadend['y'])],crs=lion.crs)
#deadend=deadend.to_crs({'init':'epsg:4326'})
#county=gpd.read_file(path+'BOUNDARY/COUNTY.shp')
#county.crs={'init':'epsg:4326'}
#deadend=gpd.sjoin(deadend,county,how='left',op='intersects')
#deadend['destreetcode']=np.where(deadend['GEOID']=='36061',116400,
#                        np.where(deadend['GEOID']=='36005',223800,
#                        np.where(deadend['GEOID']=='36047',332600,
#                        np.where(deadend['GEOID']=='36081',441700,
#                        np.where(deadend['GEOID']=='36085',525908,0)))))
#deadend=deadend.loc[deadend['destreetcode']!=0,['nodeid','destreetcode']].reset_index(drop=True)
## Find streetcode related to node
#lionstrb=lion[np.isin(lion['FeatureTyp'],['0','6','A','C','W'])].reset_index(drop=True)
#lionstrb=lionstrb[np.isin(lionstrb['SegmentTyp'],['B','R','E','T','C','S','U'])].reset_index(drop=True)
#fromnode=lionstrb[['NodeIDFrom','XFrom','YFrom','StreetCode']].reset_index(drop=True)
#fromnode['nodeid']=pd.to_numeric(fromnode['NodeIDFrom'])
#fromnode['x']=pd.to_numeric(fromnode['XFrom'])
#fromnode['y']=pd.to_numeric(fromnode['YFrom'])
#fromnode['streetcode']=pd.to_numeric(fromnode['StreetCode'])
#fromnode=fromnode[['nodeid','x','y','streetcode']].reset_index(drop=True)
#fromnodesaf=lionstrb[['NodeIDFrom','XFrom','YFrom','SAFStreetC']].reset_index(drop=True)
#fromnodesaf['nodeid']=pd.to_numeric(fromnodesaf['NodeIDFrom'])
#fromnodesaf['x']=pd.to_numeric(fromnodesaf['XFrom'])
#fromnodesaf['y']=pd.to_numeric(fromnodesaf['YFrom'])
#fromnodesaf['streetcode']=pd.to_numeric(fromnodesaf['SAFStreetC'])
#fromnodesaf=fromnodesaf.loc[pd.notna(fromnodesaf['streetcode']),['nodeid','x','y','streetcode']].reset_index(drop=True)
#tonode=lionstrb[['NodeIDTo','XTo','YTo','StreetCode']].reset_index(drop=True)
#tonode['nodeid']=pd.to_numeric(tonode['NodeIDTo'])
#tonode['x']=pd.to_numeric(tonode['XTo'])
#tonode['y']=pd.to_numeric(tonode['YTo'])
#tonode['streetcode']=pd.to_numeric(tonode['StreetCode'])
#tonode=tonode[['nodeid','x','y','streetcode']].reset_index(drop=True)
#tonodesaf=lionstrb[['NodeIDTo','XTo','YTo','SAFStreetC']].reset_index(drop=True)
#tonodesaf['nodeid']=pd.to_numeric(tonodesaf['NodeIDTo'])
#tonodesaf['x']=pd.to_numeric(tonodesaf['XTo'])
#tonodesaf['y']=pd.to_numeric(tonodesaf['YTo'])
#tonodesaf['streetcode']=pd.to_numeric(tonodesaf['SAFStreetC'])
#tonodesaf=tonodesaf.loc[pd.notna(tonodesaf['streetcode']),['nodeid','x','y','streetcode']].reset_index(drop=True)
#node=pd.concat([fromnode,fromnodesaf,tonode,tonodesaf],axis=0,ignore_index=True)
#node=node.drop_duplicates(keep='first').sort_values(['nodeid','streetcode']).reset_index(drop=True)
#node=pd.merge(node,deadend,how='left',on='nodeid')
#node['streetcode']=np.where(pd.notna(node['destreetcode']),node['destreetcode'],node['streetcode'])
#node=gpd.GeoDataFrame(node,geometry=[shapely.geometry.Point(x,y) for x,y in zip(node['x'],node['y'])],crs=lion.crs)
#node=node.to_crs({'init':'epsg:4326'})
#node['lat']=node['geometry'].y
#node['long']=node['geometry'].x
#node=node[['nodeid','lat','long','streetcode']].drop_duplicates(keep='first').sort_values(['nodeid','streetcode']).reset_index(drop=True)
#node.to_csv(path+'LION/NODE.csv',index=False)
#print(datetime.datetime.now()-start)

## Onfromto Physicalid-Streetcode-Nodeid from LION (15 mins)
#start=datetime.datetime.now()
#lion=gpd.read_file(path+'LION/LION.shp')
#lionstrb=lion[np.isin(lion['FeatureTyp'],['0','6','A','C','W'])].reset_index(drop=True)
#lionstrb=lionstrb[np.isin(lionstrb['SegmentTyp'],['B','R','E','T','C','S','U'])].reset_index(drop=True)
#onfromto=lionstrb[['PhysicalID','SeqNum','StreetCode','NodeIDFrom','NodeIDTo']].reset_index(drop=True)
#onfromto['physicalid']=pd.to_numeric(onfromto['PhysicalID'])
#onfromto['seqnum']=pd.to_numeric(onfromto['SeqNum'])
#onfromto['streetcode']=pd.to_numeric(onfromto['StreetCode'])
#onfromto['nodeidfrom']=pd.to_numeric(onfromto['NodeIDFrom'])
#onfromto['nodeidto']=pd.to_numeric(onfromto['NodeIDTo'])
#onfromto=onfromto[['physicalid','seqnum','streetcode','nodeidfrom','nodeidto']]
#onfromto=onfromto.dropna(axis=0,how='any').drop_duplicates(keep='first').reset_index(drop=True)
#onfromto=onfromto.groupby(['physicalid','streetcode'],as_index=False).apply(checkfromto).reset_index(drop=True)
#node=pd.read_csv(path+'LION/NODE.csv',dtype=float)
#onfromto=pd.merge(onfromto,node,how='left',left_on='nodeidfrom',right_on='nodeid')
#onfromto=onfromto[['physicalid','streetcode_x','streetcode_y','nodeidfrom','lat','long','nodeidto']].reset_index(drop=True)
#onfromto.columns=['physicalid','onstreetcode','fromstreetcode','nodeidfrom','latfrom','longfrom','nodeidto']
#onfromto=onfromto.groupby(['physicalid','onstreetcode'],as_index=False).apply(rmvfromst).reset_index(drop=True)
#onfromto=pd.merge(onfromto,node,how='left',left_on='nodeidto',right_on='nodeid')
#onfromto=onfromto[['physicalid','onstreetcode','fromstreetcode','nodeidfrom','latfrom','longfrom','streetcode','nodeidto','lat','long']].reset_index(drop=True)
#onfromto.columns=['physicalid','onstreetcode','fromstreetcode','nodeidfrom','latfrom','longfrom','tostreetcode','nodeidto','latto','longto']
#onfromto=onfromto.groupby(['physicalid','onstreetcode','fromstreetcode'],as_index=False).apply(rmvtost).reset_index(drop=True)
#onfromto.to_csv(path+'LION/ONFROMTO.csv',index=False)
#print(datetime.datetime.now()-start)

## Clean DOTREG LOCATION (30 mins)
#start=datetime.datetime.now()
#dotregloc=pd.read_csv(path+'DOTREG/LOCATION.csv',dtype=str)
#dotreglocclean=dotregloc.copy()
#dotreglocclean['order']=[' '.join(x.split()).upper() for x in dotreglocclean['order_no']]
#dotreglocclean['boro']=[' '.join(x.split()).upper() for x in dotreglocclean['boro']]
#dotreglocclean['boro']=np.where(dotreglocclean['boro']=='M',1,np.where(dotreglocclean['boro']=='B',2,np.where(dotreglocclean['boro']=='K',3,np.where(dotreglocclean['boro']=='Q',4,np.where(dotreglocclean['boro']=='S',5,0)))))
#dotreglocclean['onstreet']=[' '.join(x.split('*')[0].split()).upper() for x in dotreglocclean['main_st']]
#dotreglocclean['onstreetsuf']=[' '.join(x.split('*')[1].split()).upper() if len(x.split('*'))>1 else '' for x in dotreglocclean['main_st']]
#dotreglocclean['fromstreet']=[' '.join(x.split('*')[0].split()).upper() for x in dotreglocclean['from_st']]
#dotreglocclean['fromstreetsuf']=[' '.join(x.split('*')[1].split()).upper() if len(x.split('*'))>1 else '' for x in dotreglocclean['from_st']]
#dotreglocclean['tostreet']=[' '.join(x.split('*')[0].split()).upper() for x in dotreglocclean['to_st']]
#dotreglocclean['tostreetsuf']=[' '.join(x.split('*')[1].split()).upper() if len(x.split('*'))>1 else '' for x in dotreglocclean['to_st']]
#dotreglocclean['sos']=[' '.join(x.split()).upper() for x in dotreglocclean['sos']]
#dotreglocclean=dotreglocclean[['order','boro','onstreet','onstreetsuf','fromstreet','fromstreetsuf','tostreet','tostreetsuf','sos']].reset_index(drop=True)
#dotregsignclean=pd.read_csv(path+'DOTREG/SIGNCLEAN.csv',dtype=str,converters={'boro':float,'seq':float,'dist':float})
#dotregsignorder=dotregsignclean[['order']].drop_duplicates(keep='first').reset_index(drop=True)
#dotreglocclean=pd.merge(dotreglocclean,dotregsignorder,how='inner',on='order')
#snd=pd.read_csv(path+'SND/SND.csv',dtype=float,converters={'streetname':str})
#dotreglocclean=pd.merge(dotreglocclean,snd,how='left',left_on=['boro','onstreet'],right_on=['boro','streetname'])
#dotreglocclean=pd.merge(dotreglocclean,snd,how='left',left_on=['boro','fromstreet'],right_on=['boro','streetname'])
#dotreglocclean=pd.merge(dotreglocclean,snd,how='left',left_on=['boro','tostreet'],right_on=['boro','streetname'])
#dotreglocclean=dotreglocclean[['order','boro','onstreet','onstreetsuf','fromstreet','fromstreetsuf','tostreet','tostreetsuf','sos',
#                               'streetcode_x','streetcode_y','streetcode']].reset_index(drop=True)
#dotreglocclean=dotreglocclean.rename(columns={'streetcode_x':'onstreetcode','streetcode_y':'fromstreetcode','streetcode':'tostreetcode'})
#dotreglocchecksuf=dotreglocclean[(dotreglocclean['onstreetsuf']!='')|
#                                 (dotreglocclean['fromstreetsuf']!='')|
#                                 (dotreglocclean['tostreetsuf']!='')|
#                                 (pd.isna(dotreglocclean['onstreetcode']))|
#                                 (pd.isna(dotreglocclean['fromstreetcode']))|
#                                 (pd.isna(dotreglocclean['tostreetcode']))].reset_index(drop=True)
#dotreglocchecksuf.to_csv(path+'DOTREG/LOCATIONCHECKSUF.csv',index=False)
#dotreglocclean=dotreglocclean[(dotreglocclean['onstreetsuf']=='')&
#                              (dotreglocclean['fromstreetsuf']=='')&
#                              (dotreglocclean['tostreetsuf']=='')&
#                              (pd.notna(dotreglocclean['onstreetcode']))&
#                              (pd.notna(dotreglocclean['fromstreetcode']))&
#                              (pd.notna(dotreglocclean['tostreetcode']))].reset_index(drop=True)
#onfromtow=pd.read_csv(path+'LION/ONFROMTO.csv',dtype=float)
#onfromtow['geodir']='W'
#onfromtoa=pd.read_csv(path+'LION/ONFROMTO.csv',dtype=float)
#onfromtoa=onfromtoa[['physicalid','onstreetcode','tostreetcode','nodeidto','latto','longto','fromstreetcode','nodeidfrom','latfrom','longfrom']].reset_index(drop=True)
#onfromtoa.columns=['physicalid','onstreetcode','fromstreetcode','nodeidfrom','latfrom','longfrom','tostreetcode','nodeidto','latto','longto']
#onfromtoa['geodir']='A'
#onfromtowa=pd.concat([onfromtow,onfromtoa],axis=0,ignore_index=True)
#dotreglocclean=pd.merge(dotreglocclean,onfromtowa,how='left',on=['onstreetcode','fromstreetcode','tostreetcode'])
#dotreglocclean=dotreglocclean.rename(columns={'physicalid':'physicalid1','nodeidfrom':'nodeidfrom1','latfrom':'latfrom1',
#                                              'longfrom':'longfrom1','nodeidto':'nodeidto1','latto':'latto1','longto':'longto1',
#                                              'geodir':'geodir1'})
#for i in [2,3,4,5]:
#    dotreglocclean['physicalid'+str(i)]=np.nan
#    dotreglocclean['nodeidfrom'+str(i)]=np.nan
#    dotreglocclean['latfrom'+str(i)]=np.nan
#    dotreglocclean['longfrom'+str(i)]=np.nan
#    dotreglocclean['nodeidto'+str(i)]=np.nan
#    dotreglocclean['latto'+str(i)]=np.nan
#    dotreglocclean['longto'+str(i)]=np.nan
#    dotreglocclean['geodir'+str(i)]=''
#dotreglocclean['flag']=np.where(pd.notna(dotreglocclean['physicalid1']),1,np.nan)
#dotreglocclean=dotreglocclean.groupby('order',as_index=False).apply(cmbtwosegs).reset_index(drop=True)
#dotreglocclean=dotreglocclean.groupby('order',as_index=False).apply(cmbthreesegs).reset_index(drop=True)
#dotreglocclean=dotreglocclean.groupby('order',as_index=False).apply(cmbfoursegs).reset_index(drop=True)
#dotreglocclean=dotreglocclean.groupby('order',as_index=False).apply(cmbfivesegs).reset_index(drop=True)
#dotreglocchecknopid=dotreglocclean[pd.isna(dotreglocclean['flag'])].reset_index(drop=True)
#dotreglocchecknopid=dotreglocchecknopid[['order','boro','onstreet','onstreetsuf','fromstreet','fromstreetsuf','tostreet',
#                                         'tostreetsuf','sos','onstreetcode','fromstreetcode','tostreetcode']].reset_index(drop=True)
#dotreglocchecknopid.to_csv(path+'DOTREG/LOCATIONCHECKNOPID.csv',index=False)
#dotreglocclean=dotreglocclean[pd.notna(dotreglocclean['flag'])].reset_index(drop=True)
#for i in [1,2,3,4,5]:
#    dotreglocclean['bearing'+str(i)]=np.nan
#    dotreglocclean['stside'+str(i)]=''
#dotreglocclean=dotreglocclean[['order','boro','onstreet','onstreetsuf','fromstreet','fromstreetsuf','tostreet','tostreetsuf','sos',
#                               'onstreetcode','fromstreetcode','tostreetcode','physicalid1','nodeidfrom1','latfrom1','longfrom1',
#                               'nodeidto1','latto1','longto1','geodir1','bearing1','stside1','physicalid2','nodeidfrom2',
#                               'latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2','bearing2','stside2','physicalid3',
#                               'nodeidfrom3','latfrom3','longfrom3','nodeidto3','latto3','longto3','geodir3','bearing3','stside3',
#                               'physicalid4','nodeidfrom4','latfrom4','longfrom4','nodeidto4','latto4','longto4','geodir4',
#                               'bearing4','stside4','physicalid5','nodeidfrom5','latfrom5','longfrom5','nodeidto5','latto5',
#                               'longto5','geodir5','bearing5','stside5','flag']]
#dotreglocclean=dotreglocclean.apply(calcbearing,axis=1)
#dotreglocclean=dotreglocclean.apply(calcstside,axis=1)
#dotreglocclean.to_csv(path+'DOTREG/LOCATIONCLEAN.csv',index=False)
## Check SUF
#dotregloccleansuf=pd.read_csv(path+'DOTREG/LOCATIONCHECKSUF.csv',dtype=float,converters={'order':str,'onstreet':str,
#                                                                                         'onstreetsuf':str,'fromstreet':str,
#                                                                                         'fromstreetsuf':str,'tostreet':str,
#                                                                                         'tostreetsuf':str,'sos':str})
#dotregloccleansuf=pd.merge(dotregloccleansuf,onfromtowa,how='left',on=['onstreetcode','fromstreetcode','tostreetcode'])
#dotregloccleansuf=dotregloccleansuf.rename(columns={'physicalid':'physicalid1','nodeidfrom':'nodeidfrom1','latfrom':'latfrom1',
#                                                    'longfrom':'longfrom1','nodeidto':'nodeidto1','latto':'latto1',
#                                                    'longto':'longto1','geodir':'geodir1'})
#for i in [2,3,4,5]:
#    dotregloccleansuf['physicalid'+str(i)]=np.nan
#    dotregloccleansuf['nodeidfrom'+str(i)]=np.nan
#    dotregloccleansuf['latfrom'+str(i)]=np.nan
#    dotregloccleansuf['longfrom'+str(i)]=np.nan
#    dotregloccleansuf['nodeidto'+str(i)]=np.nan
#    dotregloccleansuf['latto'+str(i)]=np.nan
#    dotregloccleansuf['longto'+str(i)]=np.nan
#    dotregloccleansuf['geodir'+str(i)]=''
#dotregloccleansuf['flag']=np.where(pd.notna(dotregloccleansuf['physicalid1']),1,np.nan)
#dotregloccleansuf=dotregloccleansuf.groupby('order',as_index=False).apply(cmbtwosegs).reset_index(drop=True)
#dotregloccleansuf=dotregloccleansuf.groupby('order',as_index=False).apply(cmbthreesegs).reset_index(drop=True)
#dotregloccleansuf=dotregloccleansuf.groupby('order',as_index=False).apply(cmbfoursegs).reset_index(drop=True)
#dotregloccleansuf=dotregloccleansuf.groupby('order',as_index=False).apply(cmbfivesegs).reset_index(drop=True)
#dotreglocchecksufnopid=dotregloccleansuf[pd.isna(dotregloccleansuf['flag'])].reset_index(drop=True)
#dotreglocchecksufnopid=dotreglocchecksufnopid[['order','boro','onstreet','onstreetsuf','fromstreet','fromstreetsuf','tostreet',
#                                               'tostreetsuf','sos','onstreetcode','fromstreetcode','tostreetcode']].reset_index(drop=True)
#dotreglocchecksufnopid.to_csv(path+'DOTREG/LOCATIONCHECKSUFNOPID.csv',index=False)
#dotregloccleansuf=dotregloccleansuf[pd.notna(dotregloccleansuf['flag'])].reset_index(drop=True)
#for i in [1,2,3,4,5]:
#    dotregloccleansuf['bearing'+str(i)]=np.nan
#    dotregloccleansuf['stside'+str(i)]=''
#dotregloccleansuf=dotregloccleansuf[['order','boro','onstreet','onstreetsuf','fromstreet','fromstreetsuf','tostreet','tostreetsuf',
#                                     'sos','onstreetcode','fromstreetcode','tostreetcode','physicalid1','nodeidfrom1','latfrom1',
#                                     'longfrom1','nodeidto1','latto1','longto1','geodir1','bearing1','stside1','physicalid2',
#                                     'nodeidfrom2','latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2','bearing2',
#                                     'stside2','physicalid3','nodeidfrom3','latfrom3','longfrom3','nodeidto3','latto3','longto3',
#                                     'geodir3','bearing3','stside3','physicalid4','nodeidfrom4','latfrom4','longfrom4','nodeidto4',
#                                     'latto4','longto4','geodir4','bearing4','stside4','physicalid5','nodeidfrom5','latfrom5',
#                                     'longfrom5','nodeidto5','latto5','longto5','geodir5','bearing5','stside5','flag']]
#dotregloccleansuf=dotregloccleansuf.apply(calcbearing,axis=1)
#dotregloccleansuf=dotregloccleansuf.apply(calcstside,axis=1)
#dotregloccleansuf.to_csv(path+'DOTREG/LOCATIONCLEANSUF.csv',index=False)
#print(datetime.datetime.now()-start)

## Join LION to DOTREG LOCATION (1 mins)
#start=datetime.datetime.now()
#lion=gpd.read_file(path+'LION/LION.shp')
#liontab=lion[['PhysicalID','LBlockFace','RBlockFace']].drop_duplicates(keep='first').reset_index(drop=True)
#liontab['physicalid']=pd.to_numeric(liontab['PhysicalID'])
#liontab['lbkfaceid']=pd.to_numeric(liontab['LBlockFace'])
#liontab['rbkfaceid']=pd.to_numeric(liontab['RBlockFace'])
#liontab=liontab.loc[pd.notna(liontab['physicalid']),['physicalid','lbkfaceid','rbkfaceid']].drop_duplicates(keep='first').reset_index(drop=True)
#dotreglocclean=pd.read_csv(path+'DOTREG/LOCATIONCLEAN.csv',dtype=float,converters={'order':str,'onstreet':str,'onstreetsuf':str,
#                                                                                   'fromstreet':str,'fromstreetsuf':str,'tostreet':str,
#                                                                                   'tostreetsuf':str,'sos':str,'geodir1':str,
#                                                                                   'stside1':str,'geodir2':str,'stside2':str,
#                                                                                   'geodir3':str,'stside3':str,'geodir4':str,
#                                                                                   'stside4':str,'geodir5':str,'stside5':str})
#dotregloccleansuf=pd.read_csv(path+'DOTREG/LOCATIONCLEANSUF.csv',dtype=float,converters={'order':str,'onstreet':str,
#                                                                                         'onstreetsuf':str,'fromstreet':str,
#                                                                                         'fromstreetsuf':str,'tostreet':str,
#                                                                                         'tostreetsuf':str,'sos':str,'geodir1':str,
#                                                                                         'stside1':str,'geodir2':str,'stside2':str,
#                                                                                         'geodir3':str,'stside3':str,'geodir4':str,
#                                                                                         'stside4':str,'geodir5':str,'stside5':str})
#dotregloclion=pd.concat([dotreglocclean,dotregloccleansuf],axis=0,ignore_index=True)
#dotregloclion=pd.merge(dotreglocclean,liontab,how='left',left_on='physicalid1',right_on='physicalid')
#dotregloclion['bkfaceid1']=np.where(dotregloclion['stside1']=='L',dotregloclion['lbkfaceid'],np.where(dotregloclion['stside1']=='R',dotregloclion['rbkfaceid'],np.nan))
#dotregloclion=dotregloclion.drop(['physicalid','lbkfaceid','rbkfaceid'],axis=1).reset_index(drop=True)
#dotregloclion=pd.merge(dotregloclion,liontab,how='left',left_on='physicalid2',right_on='physicalid')
#dotregloclion['bkfaceid2']=np.where(dotregloclion['stside2']=='L',dotregloclion['lbkfaceid'],np.where(dotregloclion['stside2']=='R',dotregloclion['rbkfaceid'],np.nan))
#dotregloclion=dotregloclion.drop(['physicalid','lbkfaceid','rbkfaceid'],axis=1).reset_index(drop=True)
#dotregloclion=pd.merge(dotregloclion,liontab,how='left',left_on='physicalid3',right_on='physicalid')
#dotregloclion['bkfaceid3']=np.where(dotregloclion['stside3']=='L',dotregloclion['lbkfaceid'],np.where(dotregloclion['stside3']=='R',dotregloclion['rbkfaceid'],np.nan))
#dotregloclion=dotregloclion.drop(['physicalid','lbkfaceid','rbkfaceid'],axis=1).reset_index(drop=True)
#dotregloclion=pd.merge(dotregloclion,liontab,how='left',left_on='physicalid4',right_on='physicalid')
#dotregloclion['bkfaceid4']=np.where(dotregloclion['stside4']=='L',dotregloclion['lbkfaceid'],np.where(dotregloclion['stside4']=='R',dotregloclion['rbkfaceid'],np.nan))
#dotregloclion=dotregloclion.drop(['physicalid','lbkfaceid','rbkfaceid'],axis=1).reset_index(drop=True)
#dotregloclion=pd.merge(dotregloclion,liontab,how='left',left_on='physicalid5',right_on='physicalid')
#dotregloclion['bkfaceid5']=np.where(dotregloclion['stside5']=='L',dotregloclion['lbkfaceid'],np.where(dotregloclion['stside5']=='R',dotregloclion['rbkfaceid'],np.nan))
#dotregloclion=dotregloclion[['order','boro','onstreet','onstreetsuf','fromstreet','fromstreetsuf','tostreet','tostreetsuf','sos',
#                             'onstreetcode','fromstreetcode','tostreetcode','physicalid1','nodeidfrom1','latfrom1','longfrom1',
#                             'nodeidto1','latto1','longto1','geodir1','bearing1','stside1','bkfaceid1','physicalid2','nodeidfrom2',
#                             'latfrom2','longfrom2','nodeidto2','latto2','longto2','geodir2','bearing2','stside2','bkfaceid2',
#                             'physicalid3','nodeidfrom3','latfrom3','longfrom3','nodeidto3','latto3','longto3','geodir3','bearing3',
#                             'stside3','bkfaceid3','physicalid4','nodeidfrom4','latfrom4','longfrom4','nodeidto4','latto4','longto4',
#                             'geodir4','bearing4','stside4','bkfaceid4','physicalid5','nodeidfrom5','latfrom5','longfrom5','nodeidto5',
#                             'latto5','longto5','geodir5','bearing5','stside5','bkfaceid5','flag']]
#dotregloclion.to_csv(path+'DOTREG/LOCLION.csv',index=False)
#print(datetime.datetime.now()-start)

## Join PVMTEDGEMDN to DOTREG LOCATION (4 mins)
#start=datetime.datetime.now()
#pvmtedgemdn=gpd.read_file(path+'PLANIMETRIC/PVMTEDGEMDN.shp')
#pvmtedgemdn.crs={'init':'epsg:4326'}
#mdn=pvmtedgemdn[['bkfaceid','medians']].reset_index(drop=True)
#dotregloclion=pd.read_csv(path+'DOTREG/LOCLION.csv',dtype=float,converters={'order':str,'onstreet':str,'onstreetsuf':str,
#                                                                            'fromstreet':str,'fromstreetsuf':str,'tostreet':str,
#                                                                            'tostreetsuf':str,'sos':str,'geodir1':str,'stside1':str,
#                                                                            'geodir2':str,'stside2':str,'geodir3':str,'stside3':str,
#                                                                            'geodir4':str,'stside4':str,'geodir5':str,'stside5':str})
## Check multiple bkfaceids!!!
#dotregloclion1=dotregloclion[(dotregloclion['flag']==1)&(pd.notna(dotregloclion['bkfaceid1']))].reset_index(drop=True)
#dotregloclion1['bearing']=dotregloclion1['bearing1'].copy()
#dotregloclion1['bkfaceid']=dotregloclion1['bkfaceid1'].copy()
#dotregloclion1=pd.merge(dotregloclion1,mdn,how='left',on='bkfaceid')
#dotregloclion1=dotregloclion1.loc[dotregloclion1['medians']==0,['order','bearing','bkfaceid']].reset_index(drop=True)
#dotregloclion2=dotregloclion[(dotregloclion['flag']==2)&(dotregloclion['bkfaceid1']==dotregloclion['bkfaceid2'])].reset_index(drop=True)
#dotregloclion2['bearing']=dotregloclion2['bearing1'].copy()
#dotregloclion2['bkfaceid']=dotregloclion2['bkfaceid1'].copy()
#dotregloclion2=pd.merge(dotregloclion2,mdn,how='left',on='bkfaceid')
#dotregloclion2=dotregloclion2.loc[dotregloclion2['medians']==0,['order','bearing','bkfaceid']].reset_index(drop=True)
#dotregloclion3=dotregloclion[(dotregloclion['flag']==3)&
#                             (dotregloclion['bkfaceid1']==dotregloclion['bkfaceid2'])&
#                             (dotregloclion['bkfaceid1']==dotregloclion['bkfaceid3'])].reset_index(drop=True)
#dotregloclion3['bearing']=dotregloclion3['bearing1'].copy()
#dotregloclion3['bkfaceid']=dotregloclion3['bkfaceid1'].copy()
#dotregloclion3=pd.merge(dotregloclion3,mdn,how='left',on='bkfaceid')
#dotregloclion3=dotregloclion3.loc[dotregloclion3['medians']==0,['order','bearing','bkfaceid']].reset_index(drop=True)
#dotregloclion4=dotregloclion[(dotregloclion['flag']==4)&
#                             (dotregloclion['bkfaceid1']==dotregloclion['bkfaceid2'])&
#                             (dotregloclion['bkfaceid1']==dotregloclion['bkfaceid3'])&
#                             (dotregloclion['bkfaceid1']==dotregloclion['bkfaceid4'])].reset_index(drop=True)
#dotregloclion4['bearing']=dotregloclion4['bearing1'].copy()
#dotregloclion4['bkfaceid']=dotregloclion4['bkfaceid1'].copy()
#dotregloclion4=pd.merge(dotregloclion4,mdn,how='left',on='bkfaceid')
#dotregloclion4=dotregloclion4.loc[dotregloclion4['medians']==0,['order','bearing','bkfaceid']].reset_index(drop=True)
#dotregloclion5=dotregloclion[(dotregloclion['flag']==5)&
#                             (dotregloclion['bkfaceid1']==dotregloclion['bkfaceid2'])&
#                             (dotregloclion['bkfaceid1']==dotregloclion['bkfaceid3'])&
#                             (dotregloclion['bkfaceid1']==dotregloclion['bkfaceid4'])&
#                             (dotregloclion['bkfaceid1']==dotregloclion['bkfaceid5'])].reset_index(drop=True)
#dotregloclion5['bearing']=dotregloclion5['bearing1'].copy()
#dotregloclion5['bkfaceid']=dotregloclion5['bkfaceid1'].copy()
#dotregloclion5=pd.merge(dotregloclion5,mdn,how='left',on='bkfaceid')
#dotregloclion5=dotregloclion5.loc[dotregloclion5['medians']==0,['order','bearing','bkfaceid']].reset_index(drop=True)
#dotregloclionclean=pd.concat([dotregloclion1,dotregloclion2,dotregloclion3,dotregloclion4,dotregloclion5],axis=0,ignore_index=True)
#dotregloclionclean['brdir']=''
#dotregloclionclean=dotregloclionclean.apply(calcbrdir,axis=1)
#dotregloclionclean=dotregloclionclean.drop_duplicates(['order','bkfaceid','brdir'],keep='first').reset_index(drop=True)
#dotregloclionclean=dotregloclionclean.drop_duplicates('order',keep=False).reset_index(drop=True)
#dotregloclionpv=pd.merge(dotregloclionclean,pvmtedgemdn,how='left',on='bkfaceid')
#dotregloclionpv=dotregloclionpv[['order','bearing','bkfaceid','pvbearing','pvdir','shplen','geometry']].reset_index(drop=True)
#dotregloclionpv=dotregloclionpv.drop_duplicates('order',keep=False).reset_index(drop=True)
#dotregloclionpv=dotregloclionpv.drop_duplicates('bkfaceid',keep=False).reset_index(drop=True)
#dotregloclionpv['bearingdiff']=abs(dotregloclionpv['pvbearing']-dotregloclionpv['bearing'])
#dotregloclionpv['geom']=np.nan
#dotregloclionpv=dotregloclionpv.apply(reversegeom,axis=1)
#dotregloclionpv=dotregloclionpv.drop('geometry',axis=1).dropna(axis=0,how='any').reset_index(drop=True)
#dotregloclionpv.to_csv(path+'DOTREG/LOCLIONPV.csv',index=False)
#print(datetime.datetime.now()-start)

## Clean DOTREG Shapefile, join PVMTEDGEMDN, and keep only non DOTREG LOCATION orders/keep all orders not in LOCLIONPV (14 mins)
#start=datetime.datetime.now()
#pvmtedgemdn=gpd.read_file(path+'PLANIMETRIC/PVMTEDGEMDN.shp')
#pvmtedgemdn.crs={'init':'epsg:4326'}
#pvmtedgemdn=pvmtedgemdn.to_crs({'init':'epsg:6539'})
#dotshp=gpd.read_file(path+'DOTREG/Parking_Regulation_Shapefile/Parking_Regulation_Shapefile.shp')
#dotshp.crs={'init':'epsg:4326'}
#dotshpclean=dotshp.to_crs({'init':'epsg:6539'})
#dotshpclean['boro']=[' '.join(x.split()).upper() for x in dotshpclean['SG_KEY_BOR']]
#dotshpclean['boro']=np.where(dotshpclean['boro']=='M',1,np.where(dotshpclean['boro']=='B',2,np.where(dotshpclean['boro']=='K',3,np.where(dotshpclean['boro']=='Q',4,np.where(dotshpclean['boro']=='S',5,0)))))
#dotshpclean['order']=[' '.join(x.split()).upper() for x in dotshpclean['SG_ORDER_N']]
#dotshpclean['seq']=pd.to_numeric(dotshpclean['SG_SEQNO_N'])
#dotshpclean['arrow']=[' '.join(x.split()).upper() if pd.notna(x) else '' for x in dotshpclean['SG_ARROW_D']]
#dotshpclean['desc']=[' '.join(x.split()).upper() for x in dotshpclean['SIGNDESC1']]
#dotshpclean['mutcd']=[' '.join(x.split()).upper() for x in dotshpclean['SG_MUTCD_C']]
#dotshpclean=dotshpclean[['order','seq','geometry']].reset_index(drop=True)
#dotshpbuffer=gpd.GeoDataFrame(dotshpclean['order'],geometry=dotshpclean['geometry'].buffer(5),crs={'init':'epsg:6539'}).reset_index(drop=True)
#dotshpbuffer=gpd.sjoin(dotshpbuffer,pvmtedgemdn,how='left',op='intersects')
#dotshpbuffer=dotshpbuffer[['order','bkfaceid']].dropna().drop_duplicates(keep='first').reset_index(drop=True)
#dotshpbuffer=dotshpbuffer.drop_duplicates('order',keep=False).reset_index(drop=True)
#dotshpbuffer=dotshpbuffer.drop_duplicates('bkfaceid',keep=False).reset_index(drop=True)
#dotshpclean=pd.merge(dotshpclean,dotshpbuffer,how='inner',on='order').reset_index(drop=True)
#dotshpclean=dotshpclean.to_crs({'init':'epsg:4326'})
#dotshpclean['lat']=[x.y for x in dotshpclean['geometry']]
#dotshpclean['long']=[x.x for x in dotshpclean['geometry']]
#dotshpclean=dotshpclean[['order','seq','lat','long','bkfaceid']].sort_values(['order','seq']).reset_index(drop=True)
#pvmtedgemdn=pvmtedgemdn.to_crs({'init':'epsg:4326'})
#dotshpcleanpv=pd.merge(dotshpclean,pvmtedgemdn,how='left',on='bkfaceid')
#dotshpcleanpv['bearing']=np.nan
#dotshpcleanpv['bearingdiff']=np.nan
#dotshpcleanpv['geom']=np.nan
#dotshpcleanpv=dotshpcleanpv.groupby('order',as_index=False).apply(checkdotshpgeom).reset_index(drop=True)
### If keep only non DOTREG LOCATION orders
##dotregsign=pd.read_csv(path+'DOTREG/SIGN.csv',encoding='latin_1',dtype=str)
##dotregloc=pd.read_csv(path+'DOTREG/LOCATION.csv',dtype=str)
##dotregsignnoloc=pd.merge(dotregsign,dotregloc,how='left',left_on='SRP_Order',right_on='order_no')
##dotregsignnoloc=dotregsignnoloc[pd.isna(dotregsignnoloc['order_no'])].reset_index(drop=True)
##dotregsignnoloc['order']=[' '.join(x.split()).upper() for x in dotregsignnoloc['SRP_Order']]
##dotregsignnoloc=dotregsignnoloc[['order']].drop_duplicates(keep='first').reset_index(drop=True)
## If keep all orders not in LOCLIONPV
#dotregsignclean=pd.read_csv(path+'DOTREG/SIGNCLEAN.csv',dtype=str,converters={'boro':float,'seq':float,'dist':float})
#dotregloclionpv=pd.read_csv(path+'DOTREG/LOCLIONPV.csv',dtype=float,converters={'order':str,'pvdir':str,'geom':str})
#dotregloclionpv=dotregloclionpv[['order','bkfaceid']].drop_duplicates(keep='first').reset_index(drop=True)
#dotregsignnoloc=pd.merge(dotregsignclean,dotregloclionpv,how='left',on='order')
#dotregsignnoloc=dotregsignnoloc[pd.isna(dotregsignnoloc['bkfaceid'])].reset_index(drop=True)
#dotregsignnoloc=dotregsignnoloc[['order']].drop_duplicates(keep='first').reset_index(drop=True)
#dotshpcleanpv=pd.merge(dotshpcleanpv,dotregsignnoloc,how='inner',on='order')
#dotshpcleanpv.to_csv(path+'DOTREG/DOTSHPCLEANPV.csv',index=False)
#print(datetime.datetime.now()-start)

## Combine DOREG LOCATION and Shapefile and keep only parkable blockface (4 mins)
#start=datetime.datetime.now()
#dotregloclionpv=pd.read_csv(path+'DOTREG/LOCLIONPV.csv',dtype=float,converters={'order':str,'pvdir':str,'geom':str})
#dotshpcleanpv=pd.read_csv(path+'DOTREG/DOTSHPCLEANPV.csv',dtype=float,converters={'order':str,'pvdir':str,'geom':str})
#dotreglocshppvpk=pd.concat([dotregloclionpv,dotshpcleanpv],axis=0,ignore_index=True)
#dotreglocshppvpk=dotreglocshppvpk.drop_duplicates('order',keep=False).reset_index(drop=True)
#dotreglocshppvpk=dotreglocshppvpk.drop_duplicates('bkfaceid',keep=False).reset_index(drop=True)
#lionbfpk=pd.read_csv(path+'LION/LIONBFPK.csv',dtype=float,converters={'parking':str})
#dotreglocshppvpk=pd.merge(dotreglocshppvpk,lionbfpk,how='inner',on='bkfaceid')
#dotreglocshppvpk=dotreglocshppvpk[['order','bkfaceid','pvdir','shplen','parking','geom']].reset_index(drop=True)
#dotreglocshppvpk.to_csv(path+'DOTREG/LOCSHPPVPK.csv',index=False)
#print(datetime.datetime.now()-start)
#print(len(dotreglocshppvpk.order.unique()))
##75074/91104 82% of orders mapped at this point!!!
#dotregsignclean=pd.read_csv(path+'DOTREG/SIGNCLEAN.csv',dtype=str,converters={'boro':float,'seq':float,'dist':float})
#print(len(pd.merge(dotregsignclean,dotreglocshppvpk[['order']].drop_duplicates(),how='inner',on='order')))
##635096/761403 83% of signs mapped at this point!!!

## Join and summarize DOTREG SIGN and LOCATION + Shapefile (10000 mins)
#start=datetime.datetime.now()
#dotreglocshppvpk=pd.read_csv(path+'DOTREG/LOCSHPPVPK.csv',dtype=str,converters={'bkfaceid':float,'shplen':float})
#dotregsignclean=pd.read_csv(path+'DOTREG/SIGNCLEAN.csv',dtype=str,converters={'boro':float,'seq':float,'dist':float})
#dotregsignsum=pd.merge(dotregsignclean,dotreglocshppvpk,how='inner',on='order')
#dotregdistshplen=dotregsignsum.groupby(['order'],as_index=False).agg({'dist':'max','shplen':'mean'}).reset_index(drop=True)
#dotregdistshplen['diffabs']=abs(dotregdistshplen['dist']-dotregdistshplen['shplen'])
#dotregdistshplen['diffpct']=abs(dotregdistshplen['dist']-dotregdistshplen['shplen'])/dotregdistshplen['shplen']
#dotregdistshplen=dotregdistshplen[(dotregdistshplen['diffabs']<10)|(dotregdistshplen['diffpct']<0.1)].reset_index(drop=True)
### Criteria can be loosened!!!
#dotregdistshplen=dotregdistshplen[['order']].reset_index(drop=True)
#dotregsignsum=pd.merge(dotregsignsum,dotregdistshplen,how='inner',on='order')
#dotregsignsum=dotregsignsum[['order','seq','dist','arrow','mutcd','pvdir']].sort_values(['order','seq'],ascending=True).reset_index(drop=True)
##taglist=['w1600']
#taglist=[str(x)+str(y) for x in ['m','t','w','r','f','s','u'] for y in [str(x)[11:13]+str(x)[14:16] for x in pd.date_range('00:00','23:30',freq='30min')]]
#mutcdtagcheck=pd.read_csv(path+'DOTREG/MUTCDTAGCHECK.csv',dtype=float,converters={'mutcd':str,'orgdesc':str,'sepdesc':str,'type':str,
#                                                                                  'time':str,'starthour':str,'endhour':str})
#mutcdtagcheck=mutcdtagcheck[['mutcd']+taglist].reset_index(drop=True)
#mutcdtagchecka=mutcdtagcheck.copy()
#mutcdtagchecka['mutcd']=[str(x)+'A' for x in mutcdtagchecka['mutcd']]
#mutcdtagcheck=pd.concat([mutcdtagcheck,mutcdtagchecka],axis=0,ignore_index=True)
#mutcdhmptagcheck=pd.read_csv(path+'DOTREG/MUTCDHMPTAGCHECK.csv',dtype=float,converters={'mutcd':str,'orgdesc':str,'sepdesc':str,
#                                                                                        'type':str,'time':str,'starthour':str,
#                                                                                        'endhour':str})
#mutcdhmptagcheck=mutcdhmptagcheck[['mutcd']+taglist].reset_index(drop=True)
#mutcdhmptagcheck.columns=['mutcd']+[x+'m' for x in taglist]
#mutcdhmptagchecka=mutcdhmptagcheck.copy()
#mutcdhmptagchecka['mutcd']=[str(x)+'A' for x in mutcdhmptagchecka['mutcd']]
#mutcdhmptagcheck=pd.concat([mutcdhmptagcheck,mutcdhmptagchecka],axis=0,ignore_index=True)
#mutcdtagcheck=pd.merge(mutcdtagcheck,mutcdhmptagcheck,how='left',on='mutcd')
#mutcdtagcheck=mutcdtagcheck.fillna(0)
#for i in taglist:
#    mutcdtagcheck[i]=mutcdtagcheck[i]+mutcdtagcheck[i+'m']*100
#mutcdtagcheck=mutcdtagcheck[['mutcd']+taglist].reset_index(drop=True)
#dotregsignsum=pd.merge(dotregsignsum,mutcdtagcheck,how='left',on='mutcd')
#dotregsignsum['arrow']=dotregsignsum['arrow'].fillna('')
#dotregsignsum['cl']=np.where(dotregsignsum['mutcd']=='CL',1,0)
#dotregsignsum['blpl']=np.where(np.isin(dotregsignsum['mutcd'],['BL','PL']),1,0)
#dotregsignsum=dotregsignsum.drop(['seq','mutcd'],axis=1).reset_index(drop=True)
#dotregsignsum=dotregsignsum.groupby(['order','dist','arrow','pvdir','cl','blpl'],as_index=False).agg('sum').reset_index(drop=True)
#dotregsignsum=dotregsignsum.groupby('order',as_index=False).apply(sumsign).reset_index(drop=True)
#dotregsignsum.to_csv(path+'DOTREG/SIGNSUM.csv',index=False)
#dotregsignsum=pd.read_csv(path+'DOTREG/SIGNSUM.csv',dtype=float,converters={'order':str})
##dotregsignsum['pk']=np.where(dotregsignsum['w1600']==0,'1',np.where(dotregsignsum['w1600']>=100,'2','3'))
##taglist=[str(x)+str(y) for x in ['m','t','w','r','f','s','u'] for y in [str(x)[11:13]+str(x)[14:16] for x in pd.date_range('00:00','23:30',freq='30min')]]
##for i in taglist:
##    dotregsignsum[i]='0'
##dotregsignsum['w1600']=dotregsignsum['pk'].copy()
#for i in taglist:
#    dotregsignsum[i]=np.where(dotregsignsum[i]==0,'1',np.where(dotregsignsum[i]>=100,'2','3'))
#for i in ['m','t','w','r','f','s','u']:
#    dotregsignsum[i]=dotregsignsum[[i+str(x)[11:13]+str(x)[14:16] for x in pd.date_range('00:00','23:30',freq='30min')]].apply(lambda x: ''.join(x),axis=1)
#dotregsignsum=dotregsignsum[['order','dist1','dist2','m','t','w','r','f','s','u']].reset_index(drop=True)
#dotregsignsum.to_csv(path+'DOTREG/SIGNSUMAGG.csv',index=False)
#print(datetime.datetime.now()-start)
#print(len(dotregsignsum.order.unique()))
##69023/91104 76% of orders mapped at this point!!!
#dotregsignclean=pd.read_csv(path+'DOTREG/SIGNCLEAN.csv',dtype=str,converters={'boro':float,'seq':float,'dist':float})
#print(len(pd.merge(dotregsignclean,dotregsignsum[['order']].drop_duplicates(keep='first'),how='inner',on='order')))
##595855/761403 78% of signs mapped at this point!!!
## 1: Free Parking
## 2: Metered Parking
## 3: No Parking

## Add HYDRANT to SIGNSUM (30 mins)
#start=datetime.datetime.now()
#hydrantadj=gpd.read_file(path+'HYDRANT/HYDRANTADJ.shp')
#hydrantadj.crs={'init':'epsg:4326'}
#hydrantadj=hydrantadj.to_crs({'init':'epsg:6539'})
#dotreglocshppvpk=pd.read_csv(path+'DOTREG/LOCSHPPVPK.csv',dtype=str,converters={'bkfaceid':float,'shplen':float})
#dotreglocshppvpk=gpd.GeoDataFrame(dotreglocshppvpk,geometry=dotreglocshppvpk['geom'].map(wkt.loads),crs={'init':'epsg:4326'})
#dotreglocshppvpk=dotreglocshppvpk.to_crs({'init':'epsg:6539'})
#dotregsignsum=pd.read_csv(path+'DOTREG/SIGNSUMAGG.csv',dtype=str,converters={'dist1':float,'dist2':float})
#dotregsignsumhdt=dotregsignsum.groupby('order',as_index=False).apply(hydrantsign).reset_index(drop=True)
#dotregsignsumhdt.to_csv(path+'DOTREG/SIGNSUMAGGHDT.csv',index=False)
#print(datetime.datetime.now()-start)
#print(len(dotregsignsumhdt.order.unique()))
##69023/91104 76% of orders mapped at this point!!!
#dotregsignclean=pd.read_csv(path+'DOTREG/SIGNCLEAN.csv',dtype=str,converters={'boro':float,'seq':float,'dist':float})
#print(len(pd.merge(dotregsignclean,dotregsignsumhdt[['order']].drop_duplicates(keep='first'),how='inner',on='order')))
##595855/761403 78% of signs mapped at this point!!!

# Split geometry (20 mins)
#start=datetime.datetime.now()
#dotreglocshppvpk=pd.read_csv(path+'DOTREG/LOCSHPPVPK.csv',dtype=str,converters={'bkfaceid':float,'shplen':float})
#dotreglocshppvpk=gpd.GeoDataFrame(dotreglocshppvpk,geometry=dotreglocshppvpk['geom'].map(wkt.loads),crs={'init':'epsg:4326'})
#dotregsignsumhdt=pd.read_csv(path+'DOTREG/SIGNSUMAGGHDT.csv',dtype=str,converters={'dist1':float,'dist2':float})
#dotregsignsumhdtgm=dotregsignsumhdt.groupby('order',as_index=False).apply(splitgm).reset_index(drop=True)
#pvmtedgemdn=gpd.read_file(path+'PLANIMETRIC/PVMTEDGEMDN.shp')
#pvmtedgemdn.crs={'init':'epsg:4326'}
#pvmtedgemdn=pvmtedgemdn[['bkfaceid','medians']].reset_index(drop=True)
#dotreglocshppvpk=pd.merge(dotreglocshppvpk,pvmtedgemdn,how='left',on='bkfaceid').reset_index(drop=True)
#dotreglocshppvpk=dotreglocshppvpk[['order','medians','parking']].reset_index(drop=True)
#dotregsignsumhdtgm=pd.merge(dotregsignsumhdtgm,dotreglocshppvpk,how='left',on='order').reset_index(drop=True)
#dotregsignsumhdtgm['parkable']=1
#dotregsignsumhdtgm=dotregsignsumhdtgm[['order','dist1','dist2','m','t','w','r','f','s','u','medians','hdt','parking','parkable','geom']].reset_index(drop=True)
#dotregsignsumhdtgm.to_csv(path+'DOTREG/SIGNSUMAGGHDTGM.csv',index=False)
#dotregsignsumhdtgm=pd.read_csv(path+'DOTREG/SIGNSUMAGGHDTGM.csv',dtype=str,converters={'dist1':float,'dist2':float,'medians':float,'hdt':float,'parkable':float})
#dotregsignsumhdtgm=gpd.GeoDataFrame(dotregsignsumhdtgm,geometry=dotregsignsumhdtgm['geom'].map(wkt.loads),crs={'init':'epsg:4326'})
#dotregsignsumhdtgm=dotregsignsumhdtgm.drop('geom',axis=1).reset_index(drop=True)
#dotregsignsumhdtgm.to_file(path+'SIGNSUMAGGHDTGM.shp')
#print(datetime.datetime.now()-start)
#print(len(dotregsignsumhdtgm.order.unique()))
##68983/91104 76% of orders mapped at this point!!!
#dotregsignclean=pd.read_csv(path+'DOTREG/SIGNCLEAN.csv',dtype=str,converters={'boro':float,'seq':float,'dist':float})
#print(len(pd.merge(dotregsignclean,dotregsignsumhdtgm[['order']].drop_duplicates(keep='first'),how='inner',on='order')))
##595514/761403 78% of signs mapped at this point!!!

## Other LION blockface without DOTREG SIGN (35 mins)
#start=datetime.datetime.now()
#lionbfpk=pd.read_csv(path+'LION/LIONBFPK.csv',dtype=float,converters={'parking':str})
#dotreglocshppvpk=pd.read_csv(path+'DOTREG/LOCSHPPVPK.csv',dtype=str,converters={'bkfaceid':float,'shplen':float})
#dotreglocshppvpk=dotreglocshppvpk[['bkfaceid','order']].reset_index(drop=True)
#dotregsignsumhdtgm=pd.read_csv(path+'DOTREG/SIGNSUMAGGHDTGM.csv',dtype=str,converters={'dist1':float,'dist2':float,'hdt':float,'parkable':float})
#dotregsignsumhdtgm=dotregsignsumhdtgm[['order']].drop_duplicates(keep='first').reset_index(drop=True)
#dotreglocshppvpk=pd.merge(dotreglocshppvpk,dotregsignsumhdtgm[['order']].drop_duplicates(keep='first'),how='inner',on='order')
#lionbfpknosign=pd.merge(lionbfpk,dotreglocshppvpk,how='left',on='bkfaceid')
#lionbfpknosign=lionbfpknosign.loc[pd.isna(lionbfpknosign['order']),['bkfaceid','parking']].reset_index(drop=True)
#pvmtedgemdn=gpd.read_file(path+'PLANIMETRIC/PVMTEDGEMDN.shp')
#pvmtedgemdn.crs={'init':'epsg:4326'}
#pvmtedgemdn=pvmtedgemdn.to_crs({'init':'epsg:6539'})
#lionbfpknosign=pd.merge(lionbfpknosign,pvmtedgemdn,how='inner',on='bkfaceid')
#lionbfpknosign['dist1']=np.nan
#lionbfpknosign['dist2']=np.nan
#lionbfpknosign['hdt']=0
#hydrantadj=gpd.read_file(path+'HYDRANT/HYDRANTADJ.shp')
#hydrantadj.crs={'init':'epsg:4326'}
#hydrantadj=hydrantadj.to_crs({'init':'epsg:6539'})
#lionbfpknosignhdt=lionbfpknosign.groupby('bkfaceid',as_index=False).apply(lionpkhydrant).reset_index(drop=True)
#pvmtedgemdn=pvmtedgemdn.to_crs({'init':'epsg:4326'})
#lionbfpknosignhdtgm=lionbfpknosignhdt.groupby('bkfaceid',as_index=False).apply(lionpksplitgeom).reset_index(drop=True)
#lionbfpknosignleft=pd.merge(lionbfpknosign,lionbfpknosignhdtgm[['bkfaceid','geom']].drop_duplicates(keep='first'),how='left',on='bkfaceid')
#lionbfpknosignleft=lionbfpknosignleft[pd.isna(lionbfpknosignleft['geom'])].reset_index(drop=True)
#lionbfpknosignleft['dist1']=0
#lionbfpknosignleft['dist2']=1
#lionbfpknosignleft=gpd.GeoDataFrame(lionbfpknosignleft,geometry=lionbfpknosignleft['geometry'],crs={'init':'epsg:6539'})
#lionbfpknosignleft=lionbfpknosignleft.to_crs({'init':'epsg:4326'})
#lionbfpknosignleft['geom']=lionbfpknosignleft['geometry'].copy()
#lionbfpknosignhdtgm=pd.concat([lionbfpknosignhdtgm,lionbfpknosignleft],axis=0,ignore_index=True)
#lionbfpknosignhdtgm['parkable']=np.where(lionbfpknosignhdtgm['parking']=='0.0',0,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.0/0.1',0,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.0/0.1/0.5',0,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.0/0.1/0.5/1.0',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.0/0.1/0.9',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.0/0.1/0.9/1.0',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.0/0.1/1.0',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.0/0.5',0,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.0/0.5/0.9',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.0/0.5/0.9/1.0',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.0/0.5/1.0',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.0/0.9',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.0/0.9/1.0',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.0/1.0',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.1',0,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.1/0.5',0,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.1/0.5/0.9',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.1/0.5/1.0',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.1/0.9',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.1/0.9/1.0',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.1/1.0',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.5',0,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.5/0.9',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.5/0.9/1.0',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.5/1.0',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.9',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='0.9/1.0',1,
#                                np.where(lionbfpknosignhdtgm['parking']=='1.0',1,np.nan))))))))))))))))))))))))))))
#lionbfpknosignhdtgm=lionbfpknosignhdtgm[['bkfaceid','dist1','dist2','medians','hdt','parking','parkable','geom']].reset_index(drop=True)
#lionbfpknosignhdtgm.to_csv(path+'LION/LIONBFPKNOSIGNHDTGM.csv',index=False)
## Need to solve parking on the medians!!!
#lionbfpknosignhdtgm=pd.read_csv(path+'LION/LIONBFPKNOSIGNHDTGM.csv',dtype=float,converters={'parking':str,'geom':str})
#lionbfpknosignhdtgm=gpd.GeoDataFrame(lionbfpknosignhdtgm,geometry=lionbfpknosignhdtgm['geom'].map(wkt.loads),crs={'init':'epsg:4326'})
#lionbfpknosignhdtgm=lionbfpknosignhdtgm.drop('geom',axis=1).reset_index(drop=True)
#lionbfpknosignhdtgm.to_file(path+'LIONBFPKNOSIGNHDTGM.shp')
#print(datetime.datetime.now()-start)

## Summarize parking spaces by county (5 mins)
#start=datetime.datetime.now()
#county=gpd.read_file(path+'BOUNDARY/COUNTY.shp')
#county.crs={'init':'epsg:4326'}
#county=county.to_crs({'init':'epsg:6539'})
#county['geoid']=pd.to_numeric(county['GEOID'])
#dotregsignsumhdtgm=gpd.read_file(path+'SIGNSUMAGGHDTGM.shp')
#dotregsignsumhdtgm.crs={'init':'epsg:4326'}
#dotregsignsumhdtgm=dotregsignsumhdtgm.to_crs({'init':'epsg:6539'})
#dotregsignsumhdtgm['signsumspaces']=[int(x.length/21) for x in dotregsignsumhdtgm['geometry']]
#dotregsignsumhdtgm['sign']=np.where(dotregsignsumhdtgm['medians']==1,'3',
#                           np.where(dotregsignsumhdtgm['hdt']==1,'3',
#                           np.where(dotregsignsumhdtgm['parkable']==0,'3',[x[32] for x in dotregsignsumhdtgm['w']])))
#dotregsignsumhdtgm=gpd.sjoin(dotregsignsumhdtgm,county,how='left',op='intersects')
#dotregsignsumhdtgm=dotregsignsumhdtgm.groupby(['geoid','sign'],as_index=False).agg({'signsumspaces':'sum'}).reset_index(drop=True)
#lionbfpknosignhdtgm=gpd.read_file(path+'LIONBFPKNOSIGNHDTGM.shp')
#lionbfpknosignhdtgm.crs={'init':'epsg:4326'}
#lionbfpknosignhdtgm=lionbfpknosignhdtgm.to_crs({'init':'epsg:6539'})
#lionbfpknosignhdtgm['lionbfpknosignspaces']=[int(x.length/21) for x in lionbfpknosignhdtgm['geometry']]
#lionbfpknosignhdtgm['sign']=np.where(lionbfpknosignhdtgm['medians']==1,'3',
#                            np.where(lionbfpknosignhdtgm['hdt']==1,'3',
#                            np.where(lionbfpknosignhdtgm['parkable']==0,'3','1')))
#lionbfpknosignhdtgm=gpd.sjoin(lionbfpknosignhdtgm,county,how='left',op='intersects')
#lionbfpknosignhdtgm=lionbfpknosignhdtgm.groupby(['geoid','sign'],as_index=False).agg({'lionbfpknosignspaces':'sum'}).reset_index(drop=True)
#pkspaces=pd.merge(dotregsignsumhdtgm,lionbfpknosignhdtgm,how='left',on=['geoid','sign']).reset_index(drop=True)
#pkspaces=pkspaces.fillna(0)
#pkspaces['pkspaces']=pkspaces['signsumspaces']+pkspaces['lionbfpknosignspaces']
#pkspaces=pkspaces[['geoid','sign','pkspaces']].reset_index(drop=True)
#pkspaces['sign']=np.where(pkspaces['sign']=='1','FREE',
#                 np.where(pkspaces['sign']=='2','METERED',
#                 np.where(pkspaces['sign']=='3','NO','')))
#pkspaces=pkspaces.pivot(index='geoid',columns='sign',values='pkspaces').reset_index(drop=False)
#pkspaces.to_csv(path+'DOC/COUNTYPKSPACES.csv',index=False,na_rep=0)
#print(datetime.datetime.now()-start)

## Summarize parking spaces by community disctrict (5 mins)
#start=datetime.datetime.now()
#community=gpd.read_file(path+'BOUNDARY/COMMUNITY.shp')
#community.crs={'init':'epsg:4326'}
#community=community.to_crs({'init':'epsg:6539'})
#community['geoid']=pd.to_numeric(community['BoroCD'])
#dotregsignsumhdtgm=gpd.read_file(path+'SIGNSUMAGGHDTGM.shp')
#dotregsignsumhdtgm.crs={'init':'epsg:4326'}
#dotregsignsumhdtgm=dotregsignsumhdtgm.to_crs({'init':'epsg:6539'})
#dotregsignsumhdtgm['signsumspaces']=[int(x.length/21) for x in dotregsignsumhdtgm['geometry']]
#dotregsignsumhdtgm['sign']=np.where(dotregsignsumhdtgm['medians']==1,'3',
#                           np.where(dotregsignsumhdtgm['hdt']==1,'3',
#                           np.where(dotregsignsumhdtgm['parkable']==0,'3',[x[32] for x in dotregsignsumhdtgm['w']])))
#dotregsignsumhdtgm=gpd.sjoin(dotregsignsumhdtgm,community,how='left',op='intersects')
#dotregsignsumhdtgm=dotregsignsumhdtgm.groupby(['geoid','sign'],as_index=False).agg({'signsumspaces':'sum'}).reset_index(drop=True)
#lionbfpknosignhdtgm=gpd.read_file(path+'LIONBFPKNOSIGNHDTGM.shp')
#lionbfpknosignhdtgm.crs={'init':'epsg:4326'}
#lionbfpknosignhdtgm=lionbfpknosignhdtgm.to_crs({'init':'epsg:6539'})
#lionbfpknosignhdtgm['lionbfpknosignspaces']=[int(x.length/21) for x in lionbfpknosignhdtgm['geometry']]
#lionbfpknosignhdtgm['sign']=np.where(lionbfpknosignhdtgm['medians']==1,'3',
#                            np.where(lionbfpknosignhdtgm['hdt']==1,'3',
#                            np.where(lionbfpknosignhdtgm['parkable']==0,'3','1')))
#lionbfpknosignhdtgm=gpd.sjoin(lionbfpknosignhdtgm,community,how='left',op='intersects')
#lionbfpknosignhdtgm=lionbfpknosignhdtgm.groupby(['geoid','sign'],as_index=False).agg({'lionbfpknosignspaces':'sum'}).reset_index(drop=True)
#pkspaces=pd.merge(dotregsignsumhdtgm,lionbfpknosignhdtgm,how='left',on=['geoid','sign']).reset_index(drop=True)
#pkspaces=pkspaces.fillna(0)
#pkspaces['pkspaces']=pkspaces['signsumspaces']+pkspaces['lionbfpknosignspaces']
#pkspaces=pkspaces[['geoid','sign','pkspaces']].reset_index(drop=True)
#pkspaces['sign']=np.where(pkspaces['sign']=='1','FREE',
#                 np.where(pkspaces['sign']=='2','METERED',
#                 np.where(pkspaces['sign']=='3','NO','')))
#pkspaces=pkspaces.pivot(index='geoid',columns='sign',values='pkspaces').reset_index(drop=False)
#pkspaces.to_csv(path+'DOC/COMMUNITYPKSPACES.csv',index=False,na_rep=0)
#print(datetime.datetime.now()-start)

## Summarize parking spaces by county by time period (15 mins)
#start=datetime.datetime.now()
#taglist=[str(x)+str(y) for x in ['m','t','w','r','f','s','u'] for y in [str(x)[11:13]+str(x)[14:16] for x in pd.date_range('00:00','23:30',freq='30min')]]
#county=gpd.read_file(path+'BOUNDARY/COUNTY.shp')
#county.crs={'init':'epsg:4326'}
#county=county.to_crs({'init':'epsg:6539'})
#county['geoid']=pd.to_numeric(county['GEOID'])
#pkspaces=pd.DataFrame(columns=['time','geoid','free','metered','no'])
#pkspaces['time']=taglist*len(county)
#pkspaces['geoid']=sorted(list(county['geoid'])*len(taglist))
#dotregsignsumhdtgm=gpd.read_file(path+'SIGNSUMAGGHDTGM.shp')
#dotregsignsumhdtgm.crs={'init':'epsg:4326'}
#dotregsignsumhdtgm=dotregsignsumhdtgm.to_crs({'init':'epsg:6539'})
#dotregsignsumhdtgm['signsumspaces']=[int(x.length/21) for x in dotregsignsumhdtgm['geometry']]
#dotregsignsumhdtgm['sign']=dotregsignsumhdtgm['m']+dotregsignsumhdtgm['t']+dotregsignsumhdtgm['w']+dotregsignsumhdtgm['r']+dotregsignsumhdtgm['f']+dotregsignsumhdtgm['s']+dotregsignsumhdtgm['u']
#dotregsignsumhdtgm=gpd.sjoin(dotregsignsumhdtgm,county,how='left',op='intersects')
#dotregsignsumhdtgm=dotregsignsumhdtgm[['geoid','medians','hdt','parkable','sign','signsumspaces']].reset_index(drop=True)
#for i in range(0,len(taglist)):
#    for j in sorted(list(county['geoid'])):
#        pkspaces.loc[(pkspaces['time']==taglist[i])&(pkspaces['geoid']==j),'free']=sum(dotregsignsumhdtgm.loc[(dotregsignsumhdtgm['medians']==0)&(dotregsignsumhdtgm['hdt']==0)&(dotregsignsumhdtgm['parkable']==1)&([x[i]=='1' for x in dotregsignsumhdtgm['sign']])&(dotregsignsumhdtgm['geoid']==j),'signsumspaces'])
#        pkspaces.loc[(pkspaces['time']==taglist[i])&(pkspaces['geoid']==j),'metered']=sum(dotregsignsumhdtgm.loc[(dotregsignsumhdtgm['medians']==0)&(dotregsignsumhdtgm['hdt']==0)&(dotregsignsumhdtgm['parkable']==1)&([x[i]=='2' for x in dotregsignsumhdtgm['sign']])&(dotregsignsumhdtgm['geoid']==j),'signsumspaces'])
#        pkspaces.loc[(pkspaces['time']==taglist[i])&(pkspaces['geoid']==j),'no']=sum(dotregsignsumhdtgm.loc[((dotregsignsumhdtgm['medians']==1)|(dotregsignsumhdtgm['hdt']==1)|(dotregsignsumhdtgm['parkable']==0)|([x[i]=='3' for x in dotregsignsumhdtgm['sign']]))&(dotregsignsumhdtgm['geoid']==j),'signsumspaces'])
#lionbfpknosignhdtgm=gpd.read_file(path+'LIONBFPKNOSIGNHDTGM.shp')
#lionbfpknosignhdtgm.crs={'init':'epsg:4326'}
#lionbfpknosignhdtgm=lionbfpknosignhdtgm.to_crs({'init':'epsg:6539'})
#lionbfpknosignhdtgm['lionbfpknosignspaces']=[int(x.length/21) for x in lionbfpknosignhdtgm['geometry']]
#lionbfpknosignhdtgm['sign']=np.where(lionbfpknosignhdtgm['medians']==1,'lionno',
#                            np.where(lionbfpknosignhdtgm['hdt']==1,'lionno',
#                            np.where(lionbfpknosignhdtgm['parkable']==0,'lionno','lionfree')))
#lionbfpknosignhdtgm=gpd.sjoin(lionbfpknosignhdtgm,county,how='left',op='intersects')
#lionbfpknosignhdtgm=lionbfpknosignhdtgm.groupby(['geoid','sign'],as_index=False).agg({'lionbfpknosignspaces':'sum'}).reset_index(drop=True)
#lionbfpknosignhdtgm=lionbfpknosignhdtgm.pivot(index='geoid',columns='sign',values='lionbfpknosignspaces').reset_index(drop=False)
#pkspaces=pd.merge(pkspaces,lionbfpknosignhdtgm,how='left',on='geoid')
#pkspaces=pkspaces.fillna(0)
#pkspaces['free']=pkspaces['free']+pkspaces['lionfree']
#pkspaces['no']=pkspaces['no']+pkspaces['lionno']
#pkspaces['total']=pkspaces['free']+pkspaces['metered']
#pkspaces['time']=[str(x)+' '+str(y) for x in ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'] for y in [str(x)[11:16] for x in pd.date_range('00:00','23:30',freq='30min')]]*len(county)
#pkspaces=pkspaces[['time','geoid','free','metered','no','total']].reset_index(drop=True)
#pkspaces.to_csv(path+'DOC/COUNTYTIMEPKSPACES.csv',index=False,na_rep=0)
#print(datetime.datetime.now()-start)



# Summarize parking spaces by community district by time period (240 mins)
start=datetime.datetime.now()
taglist=[str(x)+str(y) for x in ['m','t','w','r','f','s','u'] for y in [str(x)[11:13]+str(x)[14:16] for x in pd.date_range('00:00','23:30',freq='30min')]]
community=gpd.read_file(path+'BOUNDARY/COMMUNITY.shp')
community.crs={'init':'epsg:4326'}
community=community.to_crs({'init':'epsg:6539'})
community['geoid']=pd.to_numeric(community['BoroCD'])

pkspaces=pd.DataFrame(columns=['time','geoid','free','metered','no'])
pkspaces['time']=taglist*len(community)
pkspaces['geoid']=sorted(list(community['geoid'])*len(taglist))
dotregsignsumhdtgm=gpd.read_file(path+'SIGNSUMAGGHDTGM.shp')
dotregsignsumhdtgm.crs={'init':'epsg:4326'}
dotregsignsumhdtgm=dotregsignsumhdtgm.to_crs({'init':'epsg:6539'})
dotregsignsumhdtgm['signsumspaces']=[int(x.length/21) for x in dotregsignsumhdtgm['geometry']]
dotregsignsumhdtgm['sign']=dotregsignsumhdtgm['m']+dotregsignsumhdtgm['t']+dotregsignsumhdtgm['w']+dotregsignsumhdtgm['r']+dotregsignsumhdtgm['f']+dotregsignsumhdtgm['s']+dotregsignsumhdtgm['u']
dotregsignsumhdtgm=gpd.sjoin(dotregsignsumhdtgm,community,how='left',op='intersects')
dotregsignsumhdtgm=dotregsignsumhdtgm[['geoid','medians','hdt','parkable','sign','signsumspaces']].reset_index(drop=True)
for i in range(0,len(taglist)):
    for j in sorted(list(community['geoid'])):
        pkspaces.loc[(pkspaces['time']==taglist[i])&(pkspaces['geoid']==j),'free']=sum(dotregsignsumhdtgm.loc[(dotregsignsumhdtgm['medians']==0)&(dotregsignsumhdtgm['hdt']==0)&(dotregsignsumhdtgm['parkable']==1)&([x[i]=='1' for x in dotregsignsumhdtgm['sign']])&(dotregsignsumhdtgm['geoid']==j),'signsumspaces'])
        pkspaces.loc[(pkspaces['time']==taglist[i])&(pkspaces['geoid']==j),'metered']=sum(dotregsignsumhdtgm.loc[(dotregsignsumhdtgm['medians']==0)&(dotregsignsumhdtgm['hdt']==0)&(dotregsignsumhdtgm['parkable']==1)&([x[i]=='2' for x in dotregsignsumhdtgm['sign']])&(dotregsignsumhdtgm['geoid']==j),'signsumspaces'])
        pkspaces.loc[(pkspaces['time']==taglist[i])&(pkspaces['geoid']==j),'no']=sum(dotregsignsumhdtgm.loc[((dotregsignsumhdtgm['medians']==1)|(dotregsignsumhdtgm['hdt']==1)|(dotregsignsumhdtgm['parkable']==0)|([x[i]=='3' for x in dotregsignsumhdtgm['sign']]))&(dotregsignsumhdtgm['geoid']==j),'signsumspaces'])
lionbfpknosignhdtgm=gpd.read_file(path+'LIONBFPKNOSIGNHDTGM.shp')
lionbfpknosignhdtgm.crs={'init':'epsg:4326'}
lionbfpknosignhdtgm=lionbfpknosignhdtgm.to_crs({'init':'epsg:6539'})
lionbfpknosignhdtgm['lionbfpknosignspaces']=[int(x.length/21) for x in lionbfpknosignhdtgm['geometry']]
lionbfpknosignhdtgm['sign']=np.where(lionbfpknosignhdtgm['medians']==1,'lionno',
                            np.where(lionbfpknosignhdtgm['hdt']==1,'lionno',
                            np.where(lionbfpknosignhdtgm['parkable']==0,'lionno','lionfree')))
lionbfpknosignhdtgm=gpd.sjoin(lionbfpknosignhdtgm,community,how='left',op='intersects')
lionbfpknosignhdtgm=lionbfpknosignhdtgm.groupby(['geoid','sign'],as_index=False).agg({'lionbfpknosignspaces':'sum'}).reset_index(drop=True)
lionbfpknosignhdtgm=lionbfpknosignhdtgm.pivot(index='geoid',columns='sign',values='lionbfpknosignspaces').reset_index(drop=False)
pkspaces=pd.merge(pkspaces,lionbfpknosignhdtgm,how='left',on='geoid')
pkspaces=pkspaces.fillna(0)
pkspaces['free']=pkspaces['free']+pkspaces['lionfree']
pkspaces['no']=pkspaces['no']+pkspaces['lionno']
pkspaces['total']=pkspaces['free']+pkspaces['metered']
pkspaces['time']=[str(x)+' '+str(y) for x in ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'] for y in [str(x)[11:16] for x in pd.date_range('00:00','23:30',freq='30min')]]*len(community)
pkspaces=pkspaces[['time','geoid','free','metered','no','total']].reset_index(drop=True)
pkspaces.to_csv(path+'DOC/COMMUNITYTIMEPKSPACES.csv',index=False,na_rep=0)
print(datetime.datetime.now()-start)



# Summarize parking spaces for Block 1643 and 1775 in East Harlem by time period
start=datetime.datetime.now()
taglist=[str(x)+str(y) for x in ['m','t','w','r','f','s','u'] for y in [str(x)[11:13]+str(x)[14:16] for x in pd.date_range('00:00','23:30',freq='30min')]]
eharlem=gpd.read_file(path+'REQUEST/EAST HARLEM/EAST HARLEM.shp')
eharlem.crs={'init':'epsg:4326'}
eharlem=eharlem.to_crs({'init':'epsg:6539'})
eharlem['geometry']=eharlem['geometry'].buffer(1000)
eharlem['geoid']=pd.to_numeric(eharlem['BB'])

pkspaces=pd.DataFrame(columns=['time','geoid','free','metered','no'])
pkspaces['time']=taglist*len(eharlem)
pkspaces['geoid']=sorted(list(eharlem['geoid'])*len(taglist))
dotregsignsumhdtgm=gpd.read_file(path+'SIGNSUMAGGHDTGM.shp')
dotregsignsumhdtgm.crs={'init':'epsg:4326'}
dotregsignsumhdtgm=dotregsignsumhdtgm.to_crs({'init':'epsg:6539'})
dotregsignsumhdtgm['signsumspaces']=[int(x.length/21) for x in dotregsignsumhdtgm['geometry']]
dotregsignsumhdtgm['sign']=dotregsignsumhdtgm['m']+dotregsignsumhdtgm['t']+dotregsignsumhdtgm['w']+dotregsignsumhdtgm['r']+dotregsignsumhdtgm['f']+dotregsignsumhdtgm['s']+dotregsignsumhdtgm['u']
dotregsignsumhdtgm=gpd.sjoin(dotregsignsumhdtgm,eharlem,how='inner',op='intersects')
dotregsignsumhdtgm=dotregsignsumhdtgm[['geoid','medians','hdt','parkable','sign','signsumspaces']].reset_index(drop=True)
for i in range(0,len(taglist)):
    for j in sorted(list(eharlem['geoid'])):
        pkspaces.loc[(pkspaces['time']==taglist[i])&(pkspaces['geoid']==j),'free']=sum(dotregsignsumhdtgm.loc[(dotregsignsumhdtgm['medians']==0)&(dotregsignsumhdtgm['hdt']==0)&(dotregsignsumhdtgm['parkable']==1)&([x[i]=='1' for x in dotregsignsumhdtgm['sign']])&(dotregsignsumhdtgm['geoid']==j),'signsumspaces'])
        pkspaces.loc[(pkspaces['time']==taglist[i])&(pkspaces['geoid']==j),'metered']=sum(dotregsignsumhdtgm.loc[(dotregsignsumhdtgm['medians']==0)&(dotregsignsumhdtgm['hdt']==0)&(dotregsignsumhdtgm['parkable']==1)&([x[i]=='2' for x in dotregsignsumhdtgm['sign']])&(dotregsignsumhdtgm['geoid']==j),'signsumspaces'])
        pkspaces.loc[(pkspaces['time']==taglist[i])&(pkspaces['geoid']==j),'no']=sum(dotregsignsumhdtgm.loc[((dotregsignsumhdtgm['medians']==1)|(dotregsignsumhdtgm['hdt']==1)|(dotregsignsumhdtgm['parkable']==0)|([x[i]=='3' for x in dotregsignsumhdtgm['sign']]))&(dotregsignsumhdtgm['geoid']==j),'signsumspaces'])
lionbfpknosignhdtgm=gpd.read_file(path+'LIONBFPKNOSIGNHDTGM.shp')
lionbfpknosignhdtgm.crs={'init':'epsg:4326'}
lionbfpknosignhdtgm=lionbfpknosignhdtgm.to_crs({'init':'epsg:6539'})
lionbfpknosignhdtgm['lionbfpknosignspaces']=[int(x.length/21) for x in lionbfpknosignhdtgm['geometry']]
lionbfpknosignhdtgm['sign']=np.where(lionbfpknosignhdtgm['medians']==1,'lionno',
                            np.where(lionbfpknosignhdtgm['hdt']==1,'lionno',
                            np.where(lionbfpknosignhdtgm['parkable']==0,'lionno','lionfree')))
lionbfpknosignhdtgm=gpd.sjoin(lionbfpknosignhdtgm,eharlem,how='inner',op='intersects')
lionbfpknosignhdtgm=lionbfpknosignhdtgm.groupby(['geoid','sign'],as_index=False).agg({'lionbfpknosignspaces':'sum'}).reset_index(drop=True)
lionbfpknosignhdtgm=lionbfpknosignhdtgm.pivot(index='geoid',columns='sign',values='lionbfpknosignspaces').reset_index(drop=False)
pkspaces=pd.merge(pkspaces,lionbfpknosignhdtgm,how='left',on='geoid')
pkspaces=pkspaces.fillna(0)
pkspaces['free']=pkspaces['free']+pkspaces['lionfree']
pkspaces['no']=pkspaces['no']+pkspaces['lionno']
pkspaces['total']=pkspaces['free']+pkspaces['metered']
pkspaces['time']=[str(x)+' '+str(y) for x in ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'] for y in [str(x)[11:16] for x in pd.date_range('00:00','23:30',freq='30min')]]*len(eharlem)
pkspaces=pkspaces[['time','geoid','free','metered','no','total']].reset_index(drop=True)
pkspaces.to_csv(path+'REQUEST/EAST HARLEM/EASTHARLEMTIMEPKSPACES.csv',index=False,na_rep=0)
print(datetime.datetime.now()-start)



