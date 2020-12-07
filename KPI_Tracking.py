#!/usr/bin/env python
# coding: utf-8

# In[2]:

import webbrowser
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
import seaborn as sns
from matplotlib import pyplot as plt
import warnings
import itertools
warnings.filterwarnings("ignore")


# In[3]:


#connect server
db = pypyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                        "Server=103.69.193.246;"
                        "Database=dwh;"
                        "uid=dwh_qtrr;pwd=DWH@qtrr")
print('Connected to SQL server: DWH')


# In[4]:


scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name(r'D:\F88\Python code\Immediate_warning\service_account.json', scope)
gc = gspread.authorize(credentials)


# In[5]:


write_off_id='15r30_u_xE0iUexkUgsbZqcZTy0htmSflCp-uty_1hWA'
write_off_wb=gc.open_by_key(write_off_id)

shop_detail_id='1V34xp4Dl4ROUEX_jiG-AiwPUspemhJ0MfxUz8Tvkkzg'
shop_detail_wb=gc.open_by_key(shop_detail_id)

shop_detail_2_id='1OCzHxTE7Er8_W1-IT92rTOCISAlufFOjcwtw2eIX0tU'
shop_wb=gc.open_by_key(shop_detail_2_id)

shop_update_id='1ZTQE_pfBCAUr-0GSMPJBqtCyZVxX4hdiPuT_RDrdjeQ'
shop_update_wb=gc.open_by_key(shop_update_id)




# In[6]:


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


# In[7]:


shop_control=to_dataframe(shop_detail_wb,'Quản lý KD',1,0)



today=dt.date.today().strftime('%Y-%m-%d')

today



# In[8]:


shop_data_2=to_dataframe(shop_wb,'Everything',1,0)
shop_data_2['extract_date'] = today

# shop=shop_data_2[['extract_date','Mã PGD','Tên PGD','Vùng/Miền','Tuổi','Nhóm','Tháng KT','Năm KT','Vùng', 'Phường/Xã', 'Quận/Huyện','Tỉnh/TP', 'Vĩ độ', 'Kinh độ']]

shop_data_2 =shop_data_2[['extract_date', 'TT', 'Tên PGD', 'Mã PGD', 'Trạng thái', '30d', '60d', '90d', '120d',
       '150d', '180d', '210d', '240d', '270d', '300d', '330d', '360d', 'KV',
       'Thi công', 'Nghiệm thu', 'Khai trương', 'Bạt suốt chính', 'KT',
       'Bạt suốt phụ', 'KT', 'Pano trên cao chính', 'KT', 'Pano trên cao phụ',
       'KT', 'Pano hông', 'KT', 'Số nhà', 'Phường/Xã', 'Quận/Huyện', 'Tỉnh/TP',
       'Vĩ độ', 'Kinh độ', 'Tên Google My Business locations', 'Số điện thoại',
       'Tên (POL)', 'Shop ID', 'GroupID', 'Mã partner', 'PGD Active',
       'Vùng/Miền', 'Tuổi', 'Nhóm', 'Tháng KT', 'Năm KT', 'Thời gian KT',
       'Vùng', 'QLV']]

shop_data_2['Mã PGD'] = shop_data_2['Mã PGD'].replace('', 'remove')

shop = shop_data_2[shop_data_2['Mã PGD'] != 'remove']



# In[9]:


kpi_data=to_dataframe(shop_detail_wb,'KPI for Master Report',1,0)

kpi_data['KPI Dư nợ']=kpi_data['KPI Dư nợ'].apply(lambda x: int(x.replace(',','')))

kpi_data=kpi_data[kpi_data.PGD.str.contains('Hội Sở')==False]

kpi_proces=kpi_data.groupby(['Năm KPI','Tháng KPI','PGD','Tỉnh/TP','Miền','TPK','ASM'],as_index=False).agg({'KPI Online lead':sum,'KPI Online Sale':sum,'KPI Traffic':sum,'KPI Dư nợ':sum})




# In[10]:


write_off=to_dataframe(write_off_wb,'list',1,0)

write_off.amount=write_off.amount.astype(int)

write_off.write_off_period=write_off.write_off_period.astype(int)

# Lấy danh sách write-off
write_off['code']=write_off.codeno.apply(lambda x: x[4:])




# In[11]:


def read_bal_kpi(ext_date,kpi_month):
    bal_str="declare @ext_date date='"+ext_date+"'select created,yearmonth,ContractCode,SUBSTRING(contractcode,5,len(contractcode)) as code,CustomerName,CategoryName,FromDate,ToDate,currentMoney,ShopName,ShopCode,ShopID,TuoiNo from pawnoverdue  where categoryname not like N'%vị%' and (shopcode not like N'%TEST%' or shopcode not like N'%HS%') and created=@ext_date"
    bal_data=pd.read_sql_query(bal_str,db)
    bal_merge=pd.merge(bal_data,write_off[['write_off_period','code']],on='code',how='left')
    bal_merge.loc[bal_merge.write_off_period.isna(),'write_off_period']=9999999
    bal_kpi=bal_merge[(bal_merge.write_off_period>bal_merge.yearmonth.max())&(bal_merge.tuoino<11)]
    bal_kpi_group=bal_kpi.groupby(['created','shopid','shopcode','shopname'],as_index=False).agg({'currentmoney':sum})
    bal_kpi_f=pd.merge(bal_kpi_group,kpi_proces[kpi_proces['Tháng KPI']==kpi_month],left_on='shopname',right_on='PGD',how='left')
    bal_kpi_f['bal_gap']=bal_kpi_f['KPI Dư nợ']-bal_kpi_f.currentmoney
    bal_kpi_f['complete_rate']=bal_kpi_f.currentmoney/bal_kpi_f['KPI Dư nợ']
    return bal_kpi_f



shop_data_2.head(5)


# In[12]:


date=dt.date.today()+dt.timedelta(days=-1)
ext_date=date.strftime('%Y-%m-%d')

kpi_month=str(date.month)


# In[13]:


bal_kpi=read_bal_kpi(ext_date,kpi_month)


# In[14]:


# bal_kpi[(bal_kpi['Tỉnh/TP']=='TP. Hồ Chí Minh')&(bal_kpi.complete_rate>1)]


# In[15]:


# kpi_proces[kpi_proces['Tháng KPI']=='7']


# In[16]:


write_data(shop_update_wb,'kpi','kpi',kpi_proces)


# In[17]:


write_data(shop_update_wb,'kpi_status','kpi_status',bal_kpi)


# In[18]:


write_data(shop_update_wb,'shop_detail','shop_detail',shop_control)



write_data(shop_update_wb,'shop_info','shop_info',shop)

# In[13]:


# pd.pivot_table(kpi_data[kpi_data['Tháng KPI']=='9'],index='Loại KPI Dư nợ',columns=['Miền'],values='KPI Dư nợ',aggfunc='sum',margins=True)


# In[14]:


# kpi_data[(kpi_data['Tháng KPI']=='8')&(kpi_data['Miền']=='Miền Trung')]['PGD'].unique()

webbrowser.open('https://docs.google.com/spreadsheets/d/1ZTQE_pfBCAUr-0GSMPJBqtCyZVxX4hdiPuT_RDrdjeQ/edit#gid=1626326026')

