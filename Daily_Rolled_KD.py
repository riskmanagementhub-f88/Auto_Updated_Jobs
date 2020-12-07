# Link GG sheet:https://docs.google.com/spreadsheets/d/1JcfE5m8y51P4KX2OeqzenpJH82b_vGt1L8T2uOzzxn4/edit#gid=0
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
import pygsheets
import json
from google.oauth2 import service_account

#connect server
db = pypyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                        "Server=103.69.193.246;"
                        "Database=dwh;"
                        "uid=dwh_qtrr;pwd=DWH@qtrr")
print('Connected to SQL server: DWH')

with open(r'D:\F88\Python code\Immediate_warning\service_account.json') as source:
    info = json.load(source)
credentials = service_account.Credentials.from_service_account_info(info)

client = pygsheets.authorize(service_account_file=r'D:\F88\Python code\Immediate_warning\service_account.json')

wb_id='1JcfE5m8y51P4KX2OeqzenpJH82b_vGt1L8T2uOzzxn4'
wb=client.open_by_key(wb_id)

shop_id='1OCzHxTE7Er8_W1-IT92rTOCISAlufFOjcwtw2eIX0tU'
shop_wb=client.open_by_key(shop_id)
shop_detail=shop_wb.worksheet_by_title('Everything').get_as_df()
shop=shop_detail[['Mã PGD','Vùng/Miền','Tuổi','Nhóm','Tháng KT','Năm KT','Vùng', 'Phường/Xã', 'Quận/Huyện','Tỉnh/TP', 'Vĩ độ', 'Kinh độ']]

def ext_data(numday):
    ob_day=str(-numday)
    cb_day=str(-(numday-1))
    roll_str="""
    declare @ob_date date=dateadd(day,"""+ob_day+""",getdate())
    declare @cb_date date=dateadd(day,"""+cb_day+""",getdate())
    select ob_date,cb_date,shopcode,ShopName,cate,flow,sum(CurrentMoney)trans_balance,count(PawnID)trans_pawn_count
    from (
    select tab.*,
    case when cb_bucket>ob_bucket then N'Rolled'
    when cb_bucket=ob_bucket then N'Stuck'
    else N'Closed' end as flow from (
    select @ob_date ob_date,@cb_date cb_date,ob_po.pawnid,ob_po.ContractCode,FromDate,Frequency,
    ob_po.CurrentMoney,ob_po.ShopName,ob_po.ShopCode,ob_po.TuoiNo ob_dpd,cb_po.TuoiNo cb_dpd,
    case when categoryname like N'%Đăng ký%' then N'title'
    else N'normal' end as cate,
    case when ob_po.TuoiNo<11 then 0
    else 1 end as ob_bucket,
    case when cb_po.TuoiNo is null then -1
    when cb_po.Tuoino <11 then 0
    else 1 end as cb_bucket
    from pawnoverdue ob_po
    left join (select pawnid,tuoino,created from pawnoverdue where created=@cb_date)cb_po
    on cb_po.pawnid=ob_po.pawnid
    where categoryname not like N'%vị%' and (shopcode not like N'%TEST%' or shopcode not like N'%HS%') and ob_po.created=@ob_date) tab)tab2
    where ob_bucket=0 
    group by ob_date,cb_date,ShopName,shopcode,ob_bucket,flow,cate
    """
    data=pd.read_sql_query(roll_str,db)
    normal_bal=data[data.cate=='normal'].groupby(['shopname']).agg({'trans_balance':'sum'}).reset_index()
    normal_bal.columns = ['shopname', 'normal_bal']
    normal_rolled_bal=data[(data.cate=='normal')&(data.flow=='Rolled')].groupby(['shopname']).agg({'trans_balance':'sum'}).reset_index()
    normal_rolled_bal.columns = ['shopname','normal_rolled_bal']
    normal_closed_bal=data[(data.cate=='normal')&(data.flow=='Closed')].groupby(['shopname']).agg({'trans_balance':'sum'}).reset_index()
    normal_closed_bal.columns = ['shopname','normal_closed_bal']
    title_bal=data[data.cate=='title'].groupby(['shopname']).agg({'trans_balance':'sum'}).reset_index()
    title_bal.columns = ['shopname','title_bal']
    title_rolled_bal=data[(data.cate=='title')&(data.flow=='Rolled')].groupby(['shopname']).agg({'trans_balance':'sum'}).reset_index()
    title_rolled_bal.columns = ['shopname','title_rolled_bal']
    title_closed_bal=data[(data.cate=='title')&(data.flow=='Closed')].groupby(['shopname']).agg({'trans_balance':'sum'}).reset_index()
    title_closed_bal.columns = ['shopname','title_closed_bal']
    main_data=data.groupby(['ob_date','cb_date','shopcode','shopname']).agg({'trans_balance':'sum'}).reset_index()
    main_data.columns = ['ob_date', 'cb_date', 'shopcode', 'shopname','total_bal']
    final_data=pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(pd.merge(main_data,title_bal,on='shopname',how='left'),
                      normal_bal,on='shopname',how='left'),
                    title_closed_bal,on='shopname',how='left'),
                     title_rolled_bal,on='shopname',how='left'),
             normal_closed_bal,on='shopname',how='left'),
             normal_rolled_bal,on='shopname',how='left')

    final_data=final_data.fillna(0)

    final_data['total_rolled_bal']=final_data.title_rolled_bal+final_data.normal_rolled_bal
    final_data['total_close_bal']=final_data.title_closed_bal+final_data.normal_closed_bal

    final_data=final_data[['ob_date', 'cb_date', 'shopcode', 'shopname', 'total_bal', 'title_bal',
           'normal_bal','total_close_bal', 'title_closed_bal','normal_closed_bal', 'total_rolled_bal', 'title_rolled_bal', 'normal_rolled_bal',]]
    final_data=pd.merge(final_data,shop,left_on='shopcode',right_on='Mã PGD',how='left')
    return final_data

table_data=ext_data(2)

# for i in range (2,250,1):
#     table_data=table_data.append(ext_data(i),ignore_index=True)

# table_data.to_excel(r'C:\Users\KSNB_NamTD\Desktop\rolled_daily.xlsx')

data_sheet=wb.worksheet_by_title('data')
data_sheet.clear()
data_sheet.set_dataframe(table_data,start=(1,1))

# data_sheet=wb.worksheet_by_title('historical')
# data_sheet.clear()
# data_sheet.set_dataframe(table_data,start=(1,1))

print('Job Done!')

webbrowser.open('https://docs.google.com/spreadsheets/d/1JcfE5m8y51P4KX2OeqzenpJH82b_vGt1L8T2uOzzxn4/edit#gid=0')