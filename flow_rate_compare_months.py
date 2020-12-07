#!/usr/bin/env python
# coding: utf-8

# #### Link: https://docs.google.com/spreadsheets/d/15oZa5BgMK3yQ33FYIh7XYkm1_g1KXQIGJY_fcmFCVog/edit#gid=1208285230

# In[1]:


import pandas as pd
import gspread
import gspread_dataframe as gd
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials # Đọc dữ liệu từ googlesheets
from df2gspread import df2gspread as d2g # Ghi dữ liệu lên googlesheets
from pprint import pprint
from googleapiclient import discovery
import pypyodbc #connect với sql server
import numpy as np #Tính toán (=,-,*,/)
import sqlalchemy #query trực tiếp
import urllib #connect url
import time #
import datetime as dt
import pymssql
import calendar
import warnings
import itertools
warnings.filterwarnings("ignore")


# In[2]:


import pygsheets
import json
from google.oauth2 import service_account


# In[3]:


scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
with open(r'D:\F88\Python code\Immediate_warning\service_account.json') as source:
    info = json.load(source)
credentials = service_account.Credentials.from_service_account_info(info)

client = pygsheets.authorize(service_account_file=r'D:\F88\Python code\Immediate_warning\service_account.json')


# In[4]:


scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name(r'D:\F88\Python code\Immediate_warning\service_account.json', scope)
gc = gspread.authorize(credentials)


# In[5]:


#  Lấy ID shopdetail
shop_detail_id='1ZTQE_pfBCAUr-0GSMPJBqtCyZVxX4hdiPuT_RDrdjeQ'
shop_detail_wb=gc.open_by_key(shop_detail_id)

# shopdetail_data=shop_detail_wb.worksheet_by_title('shop_info').get_as_df()

# shopdetail_data=shopdetail_data[['Mã PGD','Tháng KT','Năm KT','Vùng','Tỉnh/TP','Quận/Huyện']]


# In[6]:


wb_ib='15oZa5BgMK3yQ33FYIh7XYkm1_g1KXQIGJY_fcmFCVog'
wb=gc.open_by_key(wb_ib)


# In[7]:


#Function chuyển sheet thành Data Frame
def to_dataframe(wb_name,sheet_name,data_row,col):
    a=wb_name.worksheet(sheet_name).get_all_values()
    return pd.DataFrame(data=a[data_row:],columns=a[col])

#Funtion update data vào sheet wb_name:tên wb,target_sheet: tên sheet cần update; data:dữ liệu muốn update
def update_data(wb_name,target_sheet,data):
    existing =to_dataframe(wb_name,target_sheet,1,0)
    updated = existing.append(data)
    gd.set_with_dataframe(wb_name.worksheet(target_sheet),updated)

# Function ghi đè dữ liệu lên gg sheet
def write_data(wb_name,ws_name,ws_range,data):
    wb_name.values_clear(ws_range)
    sheet=wb_name.worksheet(ws_name)
    set_with_dataframe(sheet,data)
    print('Đã ghi dữ liệu lên sheet '+ ws_name)


# In[9]:


# Connect to server
db = pypyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                        "Server=103.69.193.246;"
                        "Database=dwh;"
                        "uid=dwh;pwd=F88!23456789")
print("Connect sucessful")


# In[10]:


def flow_cal(ob_date,cb_date):
    flow_rate_str = """declare @ob_date date='"""+ob_date+"""'
declare @cb_date date='"""+cb_date+"""'

select @ob_date from_date,@cb_date to_date,categoryname,flow,ob_bucket_name,max(ob_bucket_bal)/1000000 ob_bal,sum(CurrentMoney)/1000000 transit_bal,sum(CurrentMoney)/max(ob_bucket_bal) flow_rate,max(ob_bucket_pawn_count)ob_pawn,count(PawnID) transit_pawn_count from (
select tab.*,sum(CurrentMoney) over(partition by categoryname,ob_bucket)as ob_bucket_bal,
count(pawnid) over(partition by categoryname,ob_bucket)as ob_bucket_pawn_count, 
case when cb_bucket>ob_bucket then N'Rolled'
when cb_bucket=ob_bucket then N'Stuck'
else N'Cured' end as flow from (
select @ob_date ob_date,@cb_date cb_date,areaid,ob_po.CategoryName,ob_po.pawnid,ob_po.ContractCode,ob_po.PrincipalPaymentDay,
ob_po.ChipSerial,FromDate,dbo.getYearMonth(fromdate)brr_ym,Frequency/30 duration,
case when ob_po.PaperType like N'KT1' then ob_po.PaperType else N'KT3' end as kt
,ob_po.CurrentMoney,ob_po.ShopName,ob_po.ShopCode,
case when ob_po.PackageCode is null then N'Vay thường' else ob_po.PackageCode end as package_type,
ob_po.TuoiNo ob_dpd,cb_po.TuoiNo cb_dpd,
case 
when ob_po.TuoiNo<1 then 0
when ob_po.TuoiNo<31 and ob_po.TuoiNo>0 then 1
when ob_po.TuoiNo<61 and ob_po.TuoiNo>30 then 2
when ob_po.TuoiNo<91 and ob_po.tuoino>60 then 3
when ob_po.TuoiNo<121 and ob_po.TuoiNo>90 then 4
when ob_po.TuoiNo<151 and ob_po.TuoiNo>120 then 5
else 6 end as ob_bucket,
case 
when ob_po.TuoiNo<1 then N'0.Trong hạn'
when ob_po.TuoiNo<31 and ob_po.TuoiNo>0 then N'1.1-30 ngày'
when ob_po.TuoiNo<61 and ob_po.TuoiNo>30 then N'2.31-60 ngày'
when ob_po.TuoiNo<91 and ob_po.tuoino>60 then N'3.61-90 ngày'
when ob_po.TuoiNo<121 and ob_po.TuoiNo>90 then N'4.91-120 ngày'
when ob_po.TuoiNo<151 and ob_po.TuoiNo>120 then N'5.121-150 ngày'
else N'6.151++' end as ob_bucket_name,
case 
when cb_po.TuoiNo is null then -1
when cb_po.TuoiNo<1  then 0
when cb_po.TuoiNo<31 and cb_po.TuoiNo>0 then 1
when cb_po.TuoiNo<61 and cb_po.TuoiNo>30 then 2
when cb_po.TuoiNo<91 and cb_po.tuoino>60 then 3
when cb_po.TuoiNo<121 and cb_po.TuoiNo>90 then 4
when cb_po.TuoiNo<151 and cb_po.TuoiNo>120 then 5
else 6 end as cb_bucket,
case 
when cb_po.TuoiNo is null then N'-1. Đã đóng'
when cb_po.TuoiNo<1  then N'0.Trong hạn'
when cb_po.TuoiNo<31 and cb_po.TuoiNo>0 then N'1.1-30 ngày'
when cb_po.TuoiNo<61 and cb_po.TuoiNo>30 then N'2.31-60 ngày'
when cb_po.TuoiNo<91 and cb_po.tuoino>60 then N'3.61-90 ngày'
when cb_po.TuoiNo<121 and cb_po.TuoiNo>90 then N'4.91-120 ngày'
when cb_po.TuoiNo<151 and cb_po.TuoiNo>120 then N'5.121-150 ngày'
else N'6.151++' end as cb_bucket_name

 from pawnoverdue ob_po
 left join (select pawnid,tuoino,created from pawnoverdue where created=@cb_date)cb_po
 on cb_po.pawnid=ob_po.pawnid
 left join ShopDetail shop on shop.name=ob_po.shopname
 where CategoryName like N'%Đăng ký%' and (shopcode not like N'%TEST%' or shopcode not like N'%HS%') 
 and ob_po.created=@ob_date) tab) tab2
 group by categoryname,flow,ob_bucket_name
 order by CategoryName,ob_bucket_name"""
    return pd.read_sql_query(flow_rate_str,db)


# In[11]:


def flow_detail(ob_date,cb_date):
    flow_detail_str = """declare @ob_date date='"""+ob_date+"""'
declare @cb_date date='"""+cb_date+"""'

select * from (
select tab.*,sum(CurrentMoney) over(partition by shopname,ob_bucket)as ob_bucket_bal,
count(pawnid) over(partition by shopname,ob_bucket)as ob_bucket_pawn_count, 
case when cb_bucket>ob_bucket then N'Rolled'
when cb_bucket=ob_bucket then N'Stuck'
else N'Cured' end as flow from (
select @ob_date ob_date,@cb_date cb_date,areaid,ob_po.CategoryName,ob_po.pawnid,ob_po.ContractCode,ob_po.PrincipalPaymentDay,
ob_po.ChipSerial,FromDate,dbo.getYearMonth(fromdate)brr_ym,Frequency/30 duration,
case when ob_po.PaperType like N'KT1' then ob_po.PaperType else N'KT3' end as kt
,ob_po.CurrentMoney,ob_po.ShopName,ob_po.ShopCode,
case when ob_po.PackageCode is null then N'Vay thường' else ob_po.PackageCode end as package_type,
ob_po.TuoiNo ob_dpd,cb_po.TuoiNo cb_dpd,
case 
when ob_po.TuoiNo<1 then 0
when ob_po.TuoiNo<31 and ob_po.TuoiNo>0 then 1
when ob_po.TuoiNo<61 and ob_po.TuoiNo>30 then 2
when ob_po.TuoiNo<91 and ob_po.tuoino>60 then 3
when ob_po.TuoiNo<121 and ob_po.TuoiNo>90 then 4
when ob_po.TuoiNo<151 and ob_po.TuoiNo>120 then 5
else 6 end as ob_bucket,
case 
when ob_po.TuoiNo<1 then N'0.Trong hạn'
when ob_po.TuoiNo<31 and ob_po.TuoiNo>0 then N'1.1-30 ngày'
when ob_po.TuoiNo<61 and ob_po.TuoiNo>30 then N'2.31-60 ngày'
when ob_po.TuoiNo<91 and ob_po.tuoino>60 then N'3.61-90 ngày'
when ob_po.TuoiNo<121 and ob_po.TuoiNo>90 then N'4.91-120 ngày'
when ob_po.TuoiNo<151 and ob_po.TuoiNo>120 then N'5.121-150 ngày'
else N'6.151++' end as ob_bucket_name,
case 
when cb_po.TuoiNo is null then -1
when cb_po.TuoiNo<1  then 0
when cb_po.TuoiNo<31 and cb_po.TuoiNo>0 then 1
when cb_po.TuoiNo<61 and cb_po.TuoiNo>30 then 2
when cb_po.TuoiNo<91 and cb_po.tuoino>60 then 3
when cb_po.TuoiNo<121 and cb_po.TuoiNo>90 then 4
when cb_po.TuoiNo<151 and cb_po.TuoiNo>120 then 5
else 6 end as cb_bucket,
case 
when cb_po.TuoiNo is null then N'-1. Đã đóng'
when cb_po.TuoiNo<1  then N'0.Trong hạn'
when cb_po.TuoiNo<31 and cb_po.TuoiNo>0 then N'1.1-30 ngày'
when cb_po.TuoiNo<61 and cb_po.TuoiNo>30 then N'2.31-60 ngày'
when cb_po.TuoiNo<91 and cb_po.tuoino>60 then N'3.61-90 ngày'
when cb_po.TuoiNo<121 and cb_po.TuoiNo>90 then N'4.91-120 ngày'
when cb_po.TuoiNo<151 and cb_po.TuoiNo>120 then N'5.121-150 ngày'
else N'6.151++' end as cb_bucket_name

 from pawnoverdue ob_po
 left join (select pawnid,tuoino,created from pawnoverdue where created=@cb_date)cb_po
 on cb_po.pawnid=ob_po.pawnid
 left join ShopDetail shop on shop.name=ob_po.shopname
 where CategoryName like N'%Đăng ký%' and (shopcode not like N'%TEST%' or shopcode not like N'%HS%') and ob_po.created=@ob_date) tab)tab2
 --where flow like N'%Rolled%' and ob_bucket<4
"""
    return pd.read_sql_query(flow_detail_str,db)


# In[12]:


def rolled_per_shop(ob_date,cb_date):
    roll_shop_str="""declare @ob_date date='"""+ob_date+"""'
declare @cb_date date='"""+cb_date+"""'

select ob_date,cb_date,ShopName,ob_bucket,flow,sum(CurrentMoney)balance,max(ob_bucket_bal)bucket_bal,max(ob_bucket_pawn_count)bucket_pawn_count,count(PawnID)pawn_count,
sum(CurrentMoney)/max(ob_bucket_bal) flow_rate_bal,count(PawnID)/max(ob_bucket_pawn_count) flow_rate_pawn_count
 from (
select tab.*,sum(CurrentMoney) over(partition by shopname,ob_bucket)as ob_bucket_bal,
count(pawnid) over(partition by shopname,ob_bucket)as ob_bucket_pawn_count, 
case when cb_bucket>ob_bucket then N'Rolled'
when cb_bucket=ob_bucket then N'Stuck'
else N'Cured' end as flow from (
select @ob_date ob_date,@cb_date cb_date,ob_po.pawnid,ob_po.ContractCode,FromDate,dbo.getYearMonth(fromdate)brr_ym,Frequency,
case when ob_po.PaperType like N'KT1' then ob_po.PaperType else N'KT3' end as kt
,ob_po.CurrentMoney,ob_po.ShopName,ob_po.ShopCode,
case when ob_po.PackageCode is null then N'Vay thường' else ob_po.PackageCode end as package_type,
ob_po.TuoiNo ob_dpd,cb_po.TuoiNo cb_dpd,
case 
when ob_po.TuoiNo<1 then 0
when ob_po.TuoiNo<31 and ob_po.TuoiNo>0 then 1
when ob_po.TuoiNo<61 and ob_po.TuoiNo>5 then 2
when ob_po.TuoiNo<91 and ob_po.tuoino>10 then 3
when ob_po.TuoiNo<121 and ob_po.TuoiNo>30 then 4
when ob_po.TuoiNo<151 and ob_po.TuoiNo>60 then 5
else 6 end as ob_bucket,
case
when cb_po.TuoiNo<1 or cb_po.TuoiNo is null then 0
when ob_po.TuoiNo<31 and ob_po.TuoiNo>0 then 1
when ob_po.TuoiNo<61 and ob_po.TuoiNo>5 then 2
when ob_po.TuoiNo<91 and ob_po.tuoino>10 then 3
when ob_po.TuoiNo<121 and ob_po.TuoiNo>30 then 4
when ob_po.TuoiNo<151 and ob_po.TuoiNo>60 then 5
else 6 end as cb_bucket
 from pawnoverdue ob_po
 left join (select pawnid,tuoino,created from pawnoverdue where created=@cb_date)cb_po
 on cb_po.pawnid=ob_po.pawnid
 where categorycode like N'%17%' and categorycode like N'%15%'and shopcode not like N'%TEST%' and shopcode not like N'%HS%' and ob_po.created=@ob_date) tab)tab2
 where ob_bucket=0 and flow like N'%Rolled%'
 group by ob_date,cb_date,ShopName,ob_bucket,flow
 order by flow_rate_bal desc
"""
    
    return pd.read_sql_query(roll_shop_str,db)


# In[13]:


compare_range=5

cb_day=dt.date.today()+dt.timedelta(days=-1)
# cb_day=dt.date(2020,10,15)
# cb_day=dt.date(2020,7,31)


# In[14]:


# rolled_per_shop('2020-09-01','2020-09-30')


# In[15]:


ob_list = []
cb_list=[]
for i in range (1,compare_range):
    last_day=calendar.monthrange(cb_day.year,cb_day.month-i)[1]
    last_day_2=calendar.monthrange(cb_day.year,cb_day.month-i+1)[1]
    ob_day = dt.datetime(cb_day.year,cb_day.month-i,last_day).strftime("%Y-%m-%d")
    cb_day_2=dt.datetime(cb_day.year,cb_day.month-i+1,min(last_day_2,cb_day.day)).strftime("%Y-%m-%d")
    ob_list.append(ob_day)
    cb_list.append(cb_day_2)                     
ext_date_dict = dict(zip(ob_list, cb_list))


# In[16]:


flow_data=flow_cal(ob_list[0],ext_date_dict[ob_list[0]])
rolled_shop=rolled_per_shop(ob_list[0],ext_date_dict[ob_list[0]])
for from_date in ob_list[1:]:
    flow_data=flow_data.append(flow_cal(from_date,ext_date_dict[from_date]))
    rolled_shop=rolled_shop.append(rolled_per_shop(from_date,ext_date_dict[from_date]))


# In[17]:


# rolled=flow_data[(flow_data.flow=='Rolled')&(flow_data.categoryname.str.contains('máy'))].sort_values(by=['ob_bucket_name','from_date'],ascending=False)


# In[18]:


detail=flow_detail(ob_list[0],ext_date_dict[ob_list[0]])

flow_tab=pd.pivot_table(detail,index=['ob_date','cb_date','ob_bucket_name'],columns=['areaid','flow'],values='currentmoney',aggfunc='sum',fill_value=0).reset_index()


# In[19]:


flow_tab[('all','Cured')]=flow_tab[(1,'Cured')]+flow_tab[(2,'Cured')]
flow_tab[('all','Stuck')]=flow_tab[(1,'Stuck')]+flow_tab[(2,'Stuck')]
flow_tab[('all','Rolled')]=flow_tab[(1,'Rolled')]+flow_tab[(2,'Rolled')]

flow_tab['ob_bal_mb']=flow_tab[(1,'Rolled')]+flow_tab[(1,'Cured')]+flow_tab[(1,'Stuck')]
flow_tab['ob_bal_mn']=flow_tab[(2,'Rolled')]+flow_tab[(2,'Cured')]+flow_tab[(2,'Stuck')]
flow_tab['ob_bal_total']=flow_tab.ob_bal_mb+flow_tab.ob_bal_mn

flow_tab['rolled_rate_mb']=flow_tab[(1,'Rolled')]/flow_tab.ob_bal_mb
flow_tab['rolled_rate_mn']=flow_tab[(2,'Rolled')]/flow_tab.ob_bal_mn
flow_tab['rollea_rate_all']=flow_tab[('all','Rolled')]/flow_tab.ob_bal_total


# In[20]:


write_data(wb,'kpi_rolled','kpi_rolled',flow_tab)


# In[ ]:


write_data(wb,'rolled_detail','rolled_detail',detail)


# In[ ]:


write_data(wb,'flow_rate','flow_rate',flow_data)


# In[ ]:


write_data(wb,'rolled_shop','rolled_shop',rolled_shop)


# In[ ]:




