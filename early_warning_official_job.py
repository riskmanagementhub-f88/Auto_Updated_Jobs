#!/usr/bin/env python
# coding: utf-8

# In[137]:


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
from datetime import datetime, timedelta
import pymssql
import random
import math
import calendar
import warnings
import itertools
warnings.filterwarnings("ignore")
import pygsheets


# In[138]:


#connect server
db = pypyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                        "Server=103.69.193.246;"
                        "Database=dwh;"
                        "uid=dwh;pwd=F88!23456789")
print('Connected to SQL server: DWH')


# In[271]:


scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name(r'D:\F88\Python code\Immediate_warning\service_account.json', scope)
gc = gspread.authorize(credentials)

client = pygsheets.authorize(service_account_file=r'D:\F88\Python code\Immediate_warning\service_account.json')

#Lấy ID và truy cập wb Backup
backup_ss_id='1iYODUg3bS0JlRzkuq-cnrVZYXJgcNGAa2Wj7i2McHIc'
backup_wb=gc.open_by_key(backup_ss_id)

#Lấy ID và truy cập wb Warning.Control
control_ssheet_id= '1HqGIwrlWMaF0HX2uH7hnGglqmK2acy27-qtrTNitdRQ' 
control_wb=gc.open_by_key(control_ssheet_id)


# In[272]:


#Function chuyển sheet thành Data Frame
def to_dataframe(wb_name,sheet_name,data_row,col):
    a=wb_name.worksheet(sheet_name).get_all_values()
    return pd.DataFrame(data=a[data_row:],columns=a[col])

#Funtion update data vào sheet wb_name:tên wb,target_sheet: tên sheet cần update; data:dữ liệu muốn update
def update_data(wb_name,target_sheet,data):
    existing =to_dataframe(wb_name,target_sheet,1,0)
    updated = existing.append(data,sort=False)
    gd.set_with_dataframe(wb_name.worksheet(target_sheet),updated)

# Function ghi đè dữ liệu lên gg sheet
def write_data(wb_name,ws_name,ws_range,data):
    wb_name.values_clear(ws_range)
    sheet=wb_name.worksheet(ws_name)
    set_with_dataframe(sheet,data)
    print('Đã ghi dữ liệu lên sheet '+ ws_name)

#Function backup data các HĐ đã check
def back_up_data(data):
    update_data(backup_wb,'backup',data[['codeno','reason_check','shopcode','year_month']])
    short_list=to_dataframe(control_wb,'short_list',1,0)


# In[273]:



# collecting shop information from business department
shop_id='1OCzHxTE7Er8_W1-IT92rTOCISAlufFOjcwtw2eIX0tU'
shop_wb=client.open_by_key(shop_id)
shop_data=shop_wb.worksheet_by_title('Everything').get_as_df()


# collecting shop information from MKT department
shop_id_mkt='1ZTQE_pfBCAUr-0GSMPJBqtCyZVxX4hdiPuT_RDrdjeQ'
shop_wb_mkt=client.open_by_key(shop_id_mkt)
shop_data_mkt=shop_wb_mkt.worksheet_by_title('shop_detail').get_as_df()

shop_data_mkt = shop_data_mkt[['PGD', 'Mã PGD', 'GĐM', 'TPK', 'QLKV']]


# In[274]:


shop_data.columns


# In[275]:



shop_data = shop_data[['Mã PGD','Tên PGD','Tỉnh/TP']]


# In[276]:


# Get ASM
shop_id_mkt='1ZTQE_pfBCAUr-0GSMPJBqtCyZVxX4hdiPuT_RDrdjeQ'
shop_wb_mkt=client.open_by_key(shop_id_mkt)
marketing_shop_data=shop_wb_mkt.worksheet_by_title('shop_detail').get_as_df()

marketing_shop_data = marketing_shop_data[['PGD', 'Mã PGD', 'GĐM', 'TPK', 'QLKV']]

# Get ASM risk level
# asm_risk_level = to_dataframe(fault_group_wb, 'asm_risk_level',1,0)

today=dt.date.today()
y_m=today.strftime('%y%m')

last_day=calendar.monthrange(today.year,today.month)[1]
#Lấy ngày cuối cùng của tháng
last_day_of_month=dt.datetime(today.year,today.month,last_day)


# In[145]:


def get_n_days(nday):
    return (today+dt.timedelta(days=nday)).strftime("%Y-%m-%d")
number_day=-1

checkdate = (today+dt.timedelta(days = -1)).strftime("%Y-%m-%d")
yearmonth = (today+dt.timedelta(days = -1)).strftime("%Y%m")


# In[10]:


# Hạn mức dư quỹ


# In[11]:


cash_count_str="""Declare @ext_date date ='"""+get_n_days(number_day)+"""' 
select * 
from ( 
select cast(CREATED as date) ext_date, CREATED last_count_time,
shop,NET pos_cash_amt,MONEYRED actual_count_amt,diff,
lower(concat(NOT_MINUS,N' ',NOTE_ADD))diff_reason, 
case when diff <-10000 then N'dương quỹ'  when diff>10000 then N'âm quỹ'  else N'ok'  end as diff_type  
from CASH_AMOUNT_SHOP where cast (CREATED as date) =@ext_date) tab"""
cash_count_data=pd.read_sql_query(cash_count_str,db)


# In[12]:


# Lấy Data hạn mức dư quỹ
cash_limit=pd.read_excel(r'D:\F88\Python code\Immediate_warning\Hạn-mức-Quỹ-2020.xlsx')

cash_limit=cash_limit.rename({'PGD':'shop','Cash_Limit':'limit_amt'},axis='columns')

cash_count_data=pd.merge(cash_count_data,cash_limit[['shop','limit_amt']],on='shop',how='left')

# Đếm số từ trong giải thích nguyên nhân lệch
cash_count_data['reason_length']=cash_count_data.diff_reason.apply(lambda x: len(str(x).split(" ")))

# Check hạn mức dư quỹ
cap_2=0
cash_count_data['limit_check']='ok'
cash_count_data.loc[cash_count_data.actual_count_amt>(cash_count_data.limit_amt+cap_2),'limit_check']='Vượt hạn mức dư quỹ'

# cash_count_data.describe()

cash_count_data['diff_reason_check']='ok'
cash_count_data.loc[(cash_count_data.reason_length<3)&(cash_count_data.diff_type!='ok'),'diff_reason_check']='Không có lý do'
# Loại bỏ ký tự đặc biệt trong giải trình
cash_count_data.diff_reason=cash_count_data.diff_reason.str.replace('[^\w\s]','')
cash_count_data.loc[(cash_count_data.diff_reason.str.contains('nhầm|sai|quên|mượn|thối|nhập|mất|trộm|sự cố|lỗi'))|(cash_count_data.reason_length>=30),'diff_reason_check']='Lý do lệch không rõ ràng'

cash_count_data.loc[cash_count_data.diff_reason.str.contains('nguyên nhân|không rõ|k rõ|tìm ra|không tìm|chưa tìm|chưa rõ'),'diff_reason_check']='Lệch không rõ nguyên nhân'

# cash_count_data.loc[(cash_count_data.diff_reason.str.contains('nhầm|sai|quên|mượn|thối|nhập|mất|trộm|sự cố|lỗi'))|(cash_count_data.reason_length>=30),'diff_reason_check']='Lý do lệch không rõ ràng'

warning_treasury=cash_count_data[(cash_count_data.diff_reason_check!='ok')|(cash_count_data.limit_check!='ok')]

# Cập nhật lê google sheet
col_treasury = ['ext_date','last_count_time','shop','pos_cash_amt','actual_count_amt','diff','diff_reason','diff_type','limit_amt','reason_length','limit_check','diff_reason_check','Người check','Loại lệch quỹ','Trạng thái check',	'Kết quả','limit_amt_x','limit_amt_y']
warning_treasury = warning_treasury.reindex(columns= col_treasury )
update_data(control_wb,'Treasury',warning_treasury)


# In[13]:


# Limit HĐ active


# In[14]:


cus_exeed_pawn_limit_str="""select * 
from (select customerid,max(last_contract)last_contract,
count(CodeNo)active_pawn_count 
from (select customerid,CodeNo,
LAST_VALUE(fromdate) over( partition by customerid order by fromdate asc range between unbounded preceding and unbounded following)last_contract 
from pawn where STATUS not in (22,15,-1,1,140,11) 
and CodeNo not like N'%TEST%' and codeno not like N'%HS%')tab group by Customerid)tab2 
left join (select CustomerID,CodeNo,NAME categoryname,ShopCode,FromDate,MoneyCurrent,LoanMoneyOrg  
from pawn left join W_CATEGORY_D cate on cate.CODE=pawn.CategoryCode 
where pawn.STATUS not in (22,15,-1,1,140))pa on pa.customerid=tab2.customerid where last_contract>'2019-11-18' and active_pawn_count>4"""

pd.read_sql_query(cus_exeed_pawn_limit_str,db)

##### Limit 1 KH chỉ đc vay tối đa 1 DKXM kể từ 18-11-2019
# - Các KH vay từ 2HĐ DKXM trước đó, khi đáo hạn vẫn đc phép vay


# In[15]:


exceed_title_str=" Declare @ext_date date=dateadd(day,-1,getdate()) select po.ShopCode,ShopName,po.CustomerID,po.customername,po.PawnID,po.ContractCode,po.FromDate from PawnOverdue po left join (select * from pawn where CategoryCode like N'%17%' and STATUS =1) ri on(po.CustomerID=ri.CustomerID and po.fromdate=ri.closedate) left join (select customerid, count(pawnid) active_pawn from pawnoverdue where created=@ext_date and CategoryName like N'%Đăng ký xe máy%' group by customerid) po2 on po2.customerid=po.customerid  where categoryname like N'%Đăng ký xe máy%' and po.ShopCode not like N'%TEST%' and po.ShopCode not like N'%HS%' and po.created=@ext_date and ri.CodeNo is null and active_pawn>1 order by CustomerID"

exceed_title=pd.read_sql_query(exceed_title_str,db)

exceed_title.fromdate=pd.to_datetime(exceed_title.fromdate)

exceed_group=exceed_title.groupby(['customerid','customername'],as_index=False).agg({'pawnid':'count','fromdate':max})
col_limit = ['shopcode',	'shopname',	'customerid',	'customername',	'pawnid',	'contractcode',	'fromdate',	'checker','Loại hình sở hữu']

update_data(control_wb,'Limit',exceed_title[exceed_title.customerid.isin(exceed_group[(exceed_group.fromdate>=get_n_days(-1))].customerid)].reindex(columns = col_limit))


# In[16]:


#  HĐ thanh lý


# In[17]:


# Lấy danh sách thanh lý
yess = (dt.date.today() + dt.timedelta(days=-1)).strftime("%Y-%m-%d")
pass_pawn_str = """Declare @check_date date = '"""+yess+"""'
SELECT pawnid, [ContractCode] ma_hd,
FromDate ngay_vay
      ,[CustomerName] ten_kh
      ,[ShopName] ten_pgd
      ,[CategoryName] loai_ts
      ,[AssetDesctiption] mota_ts
      ,[TuoiNo] - 5 "so_ngay_chua_chuyen_thanh_ly" ,
	  DATEADD(d, -tuoino, created) ngay_bat_dau_qua_han,tuoino,
      CurrentMoney gia_cho_vay,AssetValue gia_tri_ts,
      created ngay_check
FROM [dwh].[dbo].[PawnOverdue] 
  where CREATED = @check_date
  and CategoryCode not like '%17%'
  and CategoryCode not like '%15%'
  and CategoryCode not like '%22%'
  and CategoryCode not like '%19%'
  and status not in (77,88,99)
  and TuoiNo>5
  order by tuoino desc"""
pass_pawn_list = pd.read_sql_query(pass_pawn_str, db)
ts_loai = to_dataframe(control_wb, 'TS_thieu',1,0)
pass_pawn_list = pass_pawn_list.merge(ts_loai, left_on= 'ma_hd', right_on= 'Mã hợp đồng', how='left')
pass_pawn_list = pass_pawn_list[pass_pawn_list['Mã hợp đồng'].isna() == True]
write_data(control_wb, 'Liquidate_pawn', 'Liquidate_pawn',pass_pawn_list[['ngay_check','ngay_bat_dau_qua_han','ten_pgd','ten_kh','pawnid','ngay_vay','ma_hd','loai_ts','mota_ts','gia_cho_vay', 'gia_tri_ts','tuoino','so_ngay_chua_chuyen_thanh_ly']])


# # In[ ]:





# # In[ ]:




# FRAUD
# In[18]:


diff_mins = '15'


# In[19]:


#  outliers selection
def upper_outlier_selection(datacolumn):
    sorted(datacolumn)
    Q1, Q3 = np.percentile(datacolumn, [25,75])
    IQR = Q3 - Q1
#     lower_range = Q1 - (1.5 * IQR)
    upper_range = Q3 + (1.5 * IQR)
    return upper_range


# In[20]:


# HĐ Mở/Đóng trước khi kiểm kê/sau khi kiểm kê => 100% cần kiểm tra


# In[21]:


open_close_before_after_cash_count_str = """Declare @check_date date = '"""+checkdate+"""'
  Declare @diff_mins int = '"""+diff_mins+"""'
   select main.*, case when (main.OpenHour <= main.min_cash_count
   or main.CloseHour <= main.min_cash_count) then N'HĐ đóng/mở trước khi kiểm kê lần đầu'
   when (main.OpenHour >= main.max_cash_count
   or main.Closehour >= main.max_cash_count) then N'HĐ đóng/mở sau khi kiểm kê lần cuối'
   when datediff(minute, main.min_cash_count, main.OpenHour) <= cast(@diff_mins as int)
   or datediff(minute, main.min_cash_count, main.CloseHour) <= cast(@diff_mins as int) then N'HĐ mở/đóng đầu ngày'
   when datediff(minute, main.OpenHour, main.max_cash_count) <= cast(@diff_mins as int) 
   or datediff(minute, main.CloseHour, main.max_cash_count) <= cast(@diff_mins as int) then N'HĐ mở/đóng cuối ngày'
   else 'Khác' end pawn_fraud_group
from (
select cas1.*,pa.ShopCode, pa.CodeNo, Customer, cd.NAME category_name, occ.OpenHour, occ.CloseHour, tp.time_processing
from (select SHOP_ID,shop_name, cast(CREATED as date) cash_count_date, 
min(CREATED) min_cash_count,
max(CREATED) max_cash_count  
   from [dwh].[dbo].w_fund_daily_f 
   where cast(CREATED as date) = @check_date
  group by SHOP_ID,shop_name ,cast(CREATED as date) ) cas1 
  left join [dwh].[dbo].Pawn pa on pa.ShopID = cas1.SHOP_ID
  left join [dwh].[dbo].OPEN_CLOSE_CONTRACT_BY_TIME  occ on occ.CodeNo = pa.CodeNo
  left join [dwh].[dbo].W_CATEGORY_D cd on cd.CODE = pa.CategoryCode 
  left join (select le.CodeNo,OpenHour, datediff(minute, InitTime, complete) time_processing
from [dwh].[dbo].OPEN_CLOSE_CONTRACT_BY_TIME le 
left join [dwh].[dbo].W_CATEGORY_D cate on cate.CODE=le.CategoryCode 
left join (select codeno,InitTime 
from [dwh].[dbo].pawn 
where status not in (15,22,11)) pa on le.CodeNo=pa.CodeNo 
left join (select PAWN_WID, CONTRACT_NO, min(CREATED) complete from [dwh].[dbo].W_PAWN_TRANSACTION_F 
where action_name like N'Cho vay' 
group by PAWN_WID,CONTRACT_NO)trans 
on trans.CONTRACT_NO=le.CodeNo where OpenHour is not null) tp on tp.CodeNo = pa.CodeNo
  where (cast(occ.OpenHour as date)= @check_date or cast(occ.CloseHour as date) = @check_date)) main
  where (datepart(hour, main.OpenHour)<=9
  and datepart(hour, main.OpenHour)>=19)
  or (datepart(hour, main.Closehour)<=9
  and datepart(hour, main.Closehour)>=19)
  """


# In[22]:


open_close_before_after_cash_count = pd.read_sql_query(open_close_before_after_cash_count_str,db)
open_close_before_after_cash_count.head(5)


# In[23]:


open_close_before_after_cash_count['extract_date'] = checkdate
open_close_before_after_cash_count['year_month'] = yearmonth


# In[24]:


checkdate


# In[25]:


full_sample_cash_count = open_close_before_after_cash_count[(open_close_before_after_cash_count['pawn_fraud_group'] =='HĐ đóng/mở sau khi kiểm kê lần cuối')|(open_close_before_after_cash_count['pawn_fraud_group'] =='HĐ đóng/mở trước khi kiểm kê lần đầu')]

full_sample_cash_count


# In[26]:


full_sample_threshold = pd.read_excel(r'D:\F88\Python code\Immediate_warning\official job\Optimize\pawn_process_theshold.xlsx', sheet_name='full_sample')
part_sample_threshold_short = pd.read_excel(r'D:\F88\Python code\Immediate_warning\official job\Optimize\pawn_process_theshold.xlsx', sheet_name='part_process_short')
part_sample_threshold_long = pd.read_excel(r'D:\F88\Python code\Immediate_warning\official job\Optimize\pawn_process_theshold.xlsx', sheet_name='part_process_long')


# In[27]:


# Danh sách HĐ đóng/mở đầu ngày cần chọn ra mẫu
part_sample_open_early = open_close_before_after_cash_count[(open_close_before_after_cash_count['pawn_fraud_group'] == 'HĐ mở/đóng đầu ngày') & (open_close_before_after_cash_count['category_name'] != 'Đăng ký Ô tô') & (open_close_before_after_cash_count['category_name'] != 'Ô tô') &  (open_close_before_after_cash_count['category_name'] != 'Xe máy')]

full_sample_threshold.columns

part_sample_open_early.columns

part_sample_open_early = part_sample_open_early.merge(full_sample_threshold, on = 'category_name', how = 'left')

part_sample_open_early['time_processing']

part_sample_open_early = part_sample_open_early[(part_sample_open_early['time_processing']<= part_sample_open_early['lower_threshold'])|(part_sample_open_early['time_processing']>= part_sample_open_early['upper_threshold'])]

part_sample_open_early


# In[28]:


# Danh sách HĐ đóng/mở cuối ngày cần chọn ra mẫu
part_sample_open_late = open_close_before_after_cash_count[(open_close_before_after_cash_count['pawn_fraud_group'] == 'HĐ mở/đóng cuối ngày')& (open_close_before_after_cash_count['category_name'] != 'Đăng ký Ô tô') & (open_close_before_after_cash_count['category_name'] != 'Ô tô') &  (open_close_before_after_cash_count['category_name'] != 'Xe máy')]

part_sample_open_late = part_sample_open_late.merge(full_sample_threshold, on = 'category_name', how = 'left')

part_sample_open_late = part_sample_open_late[(part_sample_open_late['time_processing']<= part_sample_open_late['lower_threshold'])|(part_sample_open_late['time_processing']>= part_sample_open_late['upper_threshold'])]

part_sample_open_late


# In[29]:


full_sample_cash_count = full_sample_cash_count[['year_month',
'extract_date',
'shop_name',
'shopcode',
'customer',
'codeno',
'category_name',
'pawn_fraud_group']]

part_sample_open_late = part_sample_open_late[['year_month',
'extract_date',
'shop_name',
'shopcode',
'customer',
'codeno',
'category_name',
'pawn_fraud_group']]



part_sample_open_early = part_sample_open_early[['year_month',
'extract_date',
'shop_name',
'shopcode',
'customer',
'codeno',
'category_name',
'pawn_fraud_group']]


# In[30]:


compare_to_cash_count = full_sample_cash_count.append(part_sample_open_late).append(part_sample_open_early)

compare_to_cash_count['risk_type'] = 'Gian lận'

compare_to_cash_count


# In[31]:


#  Khoảng thời gian mở HĐ


# In[32]:


open_str = """Declare @ext_date date= '"""+checkdate+"""'
select le.EXTRAC_DATE,support_staff,created_staff,ShopCode,le.CodeNo,Customer,
NAME categoryname,OpenHour,InitTime,complete, shop_name 
from [dwh].[dbo].OPEN_CLOSE_CONTRACT_BY_TIME le 
left join [dwh].[dbo].W_CATEGORY_D cate on cate.CODE=le.CategoryCode 
left join (select codeno,FirstSupportID,u.USER_CODE support_staff,u2.USER_CODE created_staff,
ShopCode,InitTime, sd.name shop_name
from [dwh].[dbo].pawn pa1 
left join [dwh].[dbo].ShopDetail sd on pa1.shopcode = sd.code
left join [dwh].[dbo].W_USER_F u on pa1.firstsupportid=u.USER_WID 
left join [dwh].[dbo].W_USER_F u2 on pa1.CreatedBy=u2.USER_WID 
where pa1.status not in (15,22,11))pa on le.CodeNo=pa.CodeNo 
left join (select PAWN_WID,CONTRACT_NO,min(CREATED)complete from [dwh].[dbo].W_PAWN_TRANSACTION_F 
where action_name like N'Cho vay' 
group by PAWN_WID,CONTRACT_NO)trans 
on trans.CONTRACT_NO=le.CodeNo where OpenHour is not null and cast(OpenHour as date) = @ext_date"""


# In[33]:


open_data=pd.read_sql_query(open_str,db)


# In[34]:


open_data['init_to_comple']=(pd.to_datetime(open_data['complete'])-open_data.inittime).dt.seconds/60

open_data.columns

open_data = open_data[(open_data['categoryname']!='Đăng ký Ô tô') & (open_data['categoryname']!='Ô tô') & (open_data['categoryname']!='Xe máy') & ((open_data['categoryname']!='Thiết bị định vị'))]

open_data['categoryname'].unique()


# In[35]:


full_sample_threshold

part_sample_threshold_short.columns = ['category_name', 'short_min', 'short_max']

part_sample_threshold_long.columns = ['category_name', 'long_min', 'long_max']

open_data = open_data.merge(full_sample_threshold, left_on = 'categoryname',right_on =  'category_name',how = 'left')

open_data = open_data.merge(part_sample_threshold_short, left_on = 'categoryname',right_on =  'category_name',how = 'left')

open_data = open_data.merge(part_sample_threshold_long, left_on = 'categoryname',right_on =  'category_name',how = 'left')


# In[36]:


full_sample_2a = open_data[open_data['init_to_comple']<=open_data['lower_threshold']]

full_sample_2b = open_data[open_data['init_to_comple']>=open_data['upper_threshold']]

full_sample_2a['pawn_fraud_group'] = 'HĐ mở trong thời gian ngắn bất thường'
full_sample_2b['pawn_fraud_group'] = 'HĐ mở trong thời gian dài bất thường'

full_sample_2 = full_sample_2a.append(full_sample_2b)

full_sample_2['year_month'] = yearmonth


# In[37]:


full_sample_2 = full_sample_2[['year_month',
'extrac_date',
'shop_name',
'shopcode',
'customer',
'codeno',
'category_name',
'pawn_fraud_group']]

full_sample_2.columns = ['year_month',
'extract_date',
'shop_name',
'shopcode',
'customer',
'codeno',
'category_name',
'pawn_fraud_group']


# In[38]:


#  Đóng HĐ trong vòng 5 ngày từ ngày mở (càng đóng sớm càng có vấn đề)
# KH cũ, không đóng vào T6, T7, CN
#  Số ngày vay >1 và nhỏ hơn 6
closed_within_5_days_str = """Declare @check_date date = '"""+checkdate+"""'
 select * from (select pa.Pawnid, pa.CodeNo,pa.shopcode, sd.Name shopname, cus.Name customer_name, pa.FromDate, pa.Todate ,pa.CloseDate, 
  datepart(weekday, pa.CloseDate) week_day, 
  case when LAG (pa.CodeNo) over (PARTITION BY pa.customerid, categorycode ORDER BY fromdate asc) is null then N'Khách mới' else N'Khách cũ' end "cus_type",
  DATEDIFF(day, pa.FromDate, pa.CloseDate) real_loan_days, cate.NAME category_name
  from [dwh].[dbo].pawn pa
  left join [dwh].[dbo].ShopDetail sd on pa.ShopCode=sd.Code
  left join [dwh].[dbo].Customer cus on cus.CustomerID = pa.CustomerID
  left join [dwh].[dbo].W_CATEGORY_D cate on cate.CODE = pa.CategoryCode 
  where pa.CloseDate is not null
  and pa.CategoryCode not like '%17%'
  and pa.CategoryCode not like '%15%'
  and DATEDIFF(day, pa.fromdate, pa.CloseDate) >1
  and DATEDIFF(day, pa.fromdate, pa.CloseDate) <=5
  and pa.closedate = @check_date
  and pa.STATUS not in (11,15,22)) main
  where main.cus_type like '%cũ%'
  and datepart(weekday, main.CloseDate) <> 1
  and datepart(weekday, main.CloseDate) <> 6
and datepart(weekday, main.CloseDate) <> 7"""


# In[39]:


closed_within_5_days = pd.read_sql_query(closed_within_5_days_str,db)


# In[40]:


closed_within_5_days['year_month'] = yearmonth
closed_within_5_days['extract_date'] = checkdate

closed_within_5_days['pawn_fraud_group'] = 'HĐ đóng trong vòng 5 ngày kể từ ngày mở'


# In[41]:


closed_within_5_days.columns = ['pawnid', 'codeno', 'shopcode', 'shop_name', 'customer', 'fromdate',
       'todate', 'closedate', 'week_day', 'cus_type', 'real_loan_days',
       'category_name', 'year_month', 'extract_date', 'pawn_fraud_group']

closed_within_5_days = closed_within_5_days[['year_month',
'extract_date',
'shop_name',
'shopcode',
'customer',
'codeno',
'category_name',
'pawn_fraud_group']]


# In[ ]:





# In[ ]:





# In[42]:


#  HĐ TS thường thanh toán khi quá hạn trên 5 ngày
overdue_close_str= """Declare @ext_date date ='"""+get_n_days(number_day)+"""' 
select pa.Pawnid,shop.Name shopname,pa.ShopCode,po.CustomerName,
pa.CodeNo,po.CategoryName,pa.FromDate,pa.CloseDate,pa.MoneyCurrent,po.TuoiNo+1 dpd 
from pawn pa 
left join PawnOverdue po on (po.PawnID=pa.Pawnid and po.CREATED=dateadd(day,-1,pa.CloseDate)) 
left join ShopDetail shop on pa.ShopCode=shop.Code 
where pa.STATUS =1 and pa.CloseDate=@ext_date 
and po.CategoryName not like N'%Đăng ký%' 
and TuoiNo>5 and pa.ShopCode not like '%DR%'
and pa.ShopCode not like '%TEST%'
and pa.ShopCode not like '%HS%'"""


# In[43]:


overdue_close_data=pd.read_sql_query(overdue_close_str,db)


# In[44]:


overdue_close_data['pawn_fraud_group'] = 'HĐ thanh toán khi quá hạn trên 5 ngày'


# In[45]:


overdue_close_data['extract_date'] = checkdate
overdue_close_data['year_month'] = yearmonth

overdue_close_data.columns = ['pawnid', 'shop_name', 'shopcode', 'customer', 'codeno',
       'category_name', 'fromdate', 'closedate', 'moneycurrent', 'dpd',
       'pawn_fraud_group', 'extract_date', 'year_month']

check_sample_4 = overdue_close_data[['year_month','extract_date','shop_name','shopcode', 'customer', 'codeno','category_name','pawn_fraud_group']]

check_sample_4


# In[46]:


fraud_full_sample_check = compare_to_cash_count.append(full_sample_2).append(closed_within_5_days).append(check_sample_4)
fraud_full_sample_check['risk_type'] ='Gian lận'
fraud_full_sample_check['codeno'].count()

fraud_full_sample_check = fraud_full_sample_check[fraud_full_sample_check['category_name'].isnull() == False]


# In[47]:


fraud_full_sample_check.columns


# In[48]:


fraud_full_sample_check = fraud_full_sample_check[['year_month','extract_date',

'shop_name',
'shopcode',
'customer',
'codeno',
'category_name',
'pawn_fraud_group',
'risk_type']]

fraud_full_sample_check.columns = ['year_month','extrac_date','shopname','shopcode','cusname','codeno','cate','reason_check','risk_type']


# In[49]:


fraud_full_sample_check.codeno.count()


# In[50]:


fraud_full_sample_check


# In[51]:


update_data(control_wb,'Checking',fraud_full_sample_check)
print("Đã update Gian lận lên sheet Checking")


# COMPLIANCE

# In[146]:


#Lấy ID và truy cập wb Backup
backup_ss_id='1iYODUg3bS0JlRzkuq-cnrVZYXJgcNGAa2Wj7i2McHIc'
backup_wb=gc.open_by_key(backup_ss_id)

#Lấy ID và truy cập wb Warning.Control
control_ssheet_id= '1HqGIwrlWMaF0HX2uH7hnGglqmK2acy27-qtrTNitdRQ' 
control_wb=gc.open_by_key(control_ssheet_id)

# Get writeoff list
writeoff_id = '15r30_u_xE0iUexkUgsbZqcZTy0htmSflCp-uty_1hWA'
writeoff_wb = client.open_by_key(writeoff_id)
write_off_list=writeoff_wb.worksheet_by_title('list').get_as_df()

write_off_list = write_off_list[['write_off_period', 'codeno', 'amount']]
write_off_list.columns = ['write_off_period', 'pawn_code', 'amount']

fault_group_id='1X7m2UnrVh909OKk9YOkxcIB455GPwTpAAcWGHIBn6_c'
fault_group_wb=gc.open_by_key(fault_group_id)

focus_shop_list = to_dataframe(fault_group_wb, 'need_focus_shop',1,0)

focus_package_list = to_dataframe(fault_group_wb, 'focus_package',1,0)


# In[147]:


#  Compliance pred_score
com_pred_score_id='1101WzQvQ897hq4xyjvKHesT4ycx8to7bD9EvLGkcPSQ'
com_pred_score_wb=gc.open_by_key(com_pred_score_id)
com_pred_score=to_dataframe(com_pred_score_wb,'Predicted_Compliance_Score',1,0)
com_pred_score['ols_pred_score'] = pd.to_numeric(com_pred_score['ols_pred_score'])

com_pred_score = com_pred_score[['shopname', 'ols_pred_score']]


# In[280]:


today=dt.date.today()
y_m= (today+dt.timedelta(days = -1)).strftime('%y%m')
checkdate = (today+dt.timedelta(days = -1)).strftime("%Y-%m-%d")
yearmonth = (today+dt.timedelta(days = -1)).strftime("%Y%m")

check_day = datetime.strptime(checkdate, '%Y-%m-%d')

check_day = check_day.day

check_day


# In[150]:


focus_shop_list.head(5)


# In[151]:


focus_shop_list['extract_date'] = checkdate

focus_package_list.columns = ['package_name', 'package_code', 'package_from_date', 'package_to_date', 'package_active_stt','package_focus_level'] 

focus_shop_list.columns

focus_shop_list.columns = ['shop_update_date', 'shop_to_date', 'shop_active_stt', 'shop', 'shop_focus_level', 'hint','updatetd_by','extract_date']

focus_package_list['extract_date'] = checkdate

focus_package_list = focus_package_list[(focus_package_list['extract_date']>=focus_package_list['package_from_date']) & (focus_package_list['extract_date']<=focus_package_list['package_to_date'])]

focus_shop_list = focus_shop_list[(focus_shop_list['extract_date']>=focus_shop_list['shop_update_date']) & (focus_shop_list['extract_date']<=focus_shop_list['shop_to_date'])]


# In[152]:


focus_shop_list


# In[155]:


focus_shop_lastest = focus_shop_list.groupby('shop').agg({'shop_update_date':'max'}).reset_index()


# In[156]:


try:
    focus_shop_lastest = focus_shop_lastest.merge(focus_shop_list, on = ['shop', 'shop_update_date'], how = 'left')
except Exception:
    pass


# In[157]:


focus_shop_lastest.head(5)


# In[158]:


# focus_shop_lastest = focus_shop_list.groupby('shop').agg({'shop_update_date':'max'}).reset_index()

# focus_shop_lastest = focus_shop_lastest.merge(focus_shop_list, on = ['shop', 'shop_update_date'], how = 'left')


# In[160]:


# focus_shop_lastest = focus_shop_lastest[['shop','shop_update_date']]

# focus_shop_lastest


# In[170]:


#  Shop Check History
shop_check_id = '1HqGIwrlWMaF0HX2uH7hnGglqmK2acy27-qtrTNitdRQ'
shop_check_wb = client.open_by_key(shop_check_id)


# In[171]:


yearmonth = (today+dt.timedelta(days = -1)).strftime("%Y%m")


# In[172]:


shop_check_history = shop_check_wb.worksheet_by_title('Checking').get_as_df()


# In[174]:


shop_check_history.head(5)


# In[175]:


shop_check_history['year_month'] = [''.join(x.split('-')[0:2]) for x in shop_check_history['extrac_date']]

shop_check_history['year_month'] = shop_check_history['year_month'].astype(str)

shop_check_history = shop_check_history[(shop_check_history['year_month'] == yearmonth) & (shop_check_history['risk_type'] == 'Tuân thủ')]

shop_check_history = shop_check_history.groupby('shopname').agg({'extrac_date': 'nunique', 'codeno':'count'}).reset_index()


# In[177]:


shop_check_history['nums_day_in_month'] = check_day

shop_check_history['shop_check_rate'] = shop_check_history['extrac_date']/shop_check_history['nums_day_in_month']

shop_check_history['average_nums_checked_pawn'] = shop_check_history['codeno']/shop_check_history['nums_day_in_month']

shop_check_history.columns = ['shopname', 'shop_nums_checked_day', 'total_num_checked_pawn', 'nums_day_in_month', 'shop_checked_rate', 'average_nums_checked_pawn']


# In[178]:


# sns.boxplot(shop_check_history.average_nums_checked_pawn)

# shop_check_history.average_nums_checked_pawn.describe()

# shop_check_history.columns

shop_check_history.columns = ['shopname', 'shop_nums_checked_day', 'total_num_checked_pawn', 'nums_day_in_month', 'shop_checked_rate', 'average_nums_checked_pawn']


# In[179]:


#  Score card
score_card_id = '1101WzQvQ897hq4xyjvKHesT4ycx8to7bD9EvLGkcPSQ'
score_card_wb = client.open_by_key(score_card_id)
score_card_categorical=score_card_wb.worksheet_by_title('score_card_categorical').get_as_df()

score_card_numeric=score_card_wb.worksheet_by_title('score_card_numeric').get_as_df()


# In[180]:


pawn_infor_str = """Declare @check_date date = '"""+checkdate+"""'
  select @check_date extract_date,pod.customer_name customer_name,pa.ShopCode shopcode,pa.packagecode,pa.fromdate, pa.Pawnid, pa.CodeNo,cad.NAME category_name , case when pa.PackageCode like '%NHANH%' then N'Vay nhanh' else N'Vay thường' end package_name,
  pod.PaperType,pod.ltv_index ,(pa.LoanMoneyOrg - pa.InsurranceMoney) net_disbur,
    case when pa.CategoryCode like '%17%' then
		case when (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 3000000 then '0. <=3tr'
		when (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 10000000 then '1. 3-10tr'
		when (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 15000000 then '2. 10-15tr'
		when (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 25000000 then '3. 15-25tr'
		else '4. >25tr' end 
  when pa.CategoryCode like '%15%' then
		case when (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 50000000 then '0. <=50tr'
		when (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 100000000 then '2. 100-200tr'
		when pa.PackageCode is not null and  (pa.LoanMoneyOrg - pa.[InsurranceMoney]) > 200000000 then N'4. >200tr'
		when pa.PackageCode is null and  (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 300000000 then '3. 200-300tr'
		else N'4. >300tr' end 
 end package_money_range
	, case when frequency <= 30 then N'1. Kỳ hạn 1 tháng'
  when pa.frequency <= 90 then N'2. Kỳ hạn 3 tháng'
  when pa.frequency <= 180 then N'3. Kỳ hạn 6 tháng'
  when pa.frequency <= 270 then N'4. Kỳ hạn 9 tháng'
  when pa.frequency <= 360 then N'5. Kỳ hạn 12 tháng'
  else N'6. >12 tháng' end package_time_range,
  case when pa.previous_pawn is null then N'Khách mới' else N'Khách cũ' end cus_type,
case when pa.previous_pawn_status is null then N'KH mới vay lần đầu' 
when pa.previous_pawn_status in (77,88,99) then N'KH đang nợ xấu'
when pa.previous_pawn_status = 14 then N'7.Vay thêm'
when pa.previous_pawn_status = 1 and pa.pre_pawn_last_close_day = pa.FromDate then N'0.Đáo hạn'
else (
case when pa.sleep_day>180 then N'5.Mở lại từ 181 ngày'
when pa.sleep_day>90 then N'4.Mở lại từ 91-180 ngày' 
when pa.sleep_day> 60 then N'3.Mở lại từ 61-90 ngày'
when pa.sleep_day> 30 then N'2.Mở lại từ 31-60 ngày'
else N'1.Mở lại từ 1-30 ngày' end ) end open_type,
 pa2.num_created_pawn, csh.CUSTOMER_SCORE, 
   ima.nums_image, trans.complete, occ.OpenHour,  datepart(hour, occ.OpenHour) open_hour_extracted,DATEDIFF(minute, pa.InitTime, trans.complete) processing_minute,
    ima.first_upload, ima.last_upload,DATEDIFF(minute,  occ.OpenHour,ima.first_upload) first_upload_to_open,  DATEDIFF(minute,  occ.OpenHour,ima.last_upload) last_upload_to_open
  from (select *,  LAG (CodeNo) over (PARTITION BY customerid, CategoryCode ORDER BY fromdate asc) previous_pawn,
   LAG (STATUS) over (PARTITION BY customerid ORDER BY fromdate asc) previous_pawn_status,
   Lag(CloseDate) over (PARTITION BY customerid, [CategoryCode] order by fromdate) as pre_pawn_last_close_day,
   DATEDIFF(d, Lag(CloseDate) over (PARTITION BY customerid order by fromdate),Fromdate) sleep_day
  from [dwh].[dbo].pawn 
  where FromDate = @check_date
  and STATUS in (1,14))pa
  left join [dwh].[dbo].ShopDetail sd on sd.code = pa.ShopCode
  left join [dwh].[dbo].W_CATEGORY_D cad on cad.CODE = pa.CategoryCode
  left join [dwh].[dbo].OPEN_CLOSE_CONTRACT_BY_TIME occ on occ.CodeNo = pa.CodeNo
  left join ( select b.CUSTOMERID, b.CUSTOMER_SCORE
  from (select CUSTOMERID, max(DATA_DATE) max_date
  FROM [dwh].[dbo].[CustomerScore_History]
  group by CUSTOMERID) a
  left join [dwh].[dbo].[CustomerScore_History] b on (a.CUSTOMERID = b.CUSTOMERID and b.DATA_DATE = a.max_date)) csh on csh.CUSTOMERID = pa.CustomerID
  left join (
  select FromDate, shopcode, count(pawnid) num_created_pawn /*, case when CategoryCode like '%15%' then 'DKOT'
  when CategoryCode like '%17%' then 'DKXM' else 'SPTT' end categoryname, */
  from [dwh].[dbo].pawn
  where FromDate = @check_date
  and status in (1,14)
  group by FromDate, ShopCode
  ) pa2 on (pa.ShopCode = pa2.ShopCode and pa.Fromdate = pa2.Fromdate)
left join (select paf.PawnID, count(paf.PawnAssetFileID) nums_image, max(paf.Datetime) last_upload, min(paf.Datetime) first_upload
from [dwh].[dbo].PawnAssetFile_view paf
left join [dwh].[dbo].pawn po on po.Pawnid= paf.PawnID
where FromDate = @check_date
and po.STATUS in (1,14)
group by paf.PawnID) ima on ima.PawnID = pa.pawnid
left join (select PAWN_WID,CONTRACT_NO,min(CREATED)complete from [dwh].[dbo].W_PAWN_TRANSACTION_F 
where action_name like N'Cho vay' 
group by PAWN_WID,CONTRACT_NO) trans on trans.CONTRACT_NO=pa.CodeNo
left join (select pawnid, PaperType,(moneyOrg - InsurranceMoney)/MoneyAppraisal ltv_index, CustomerName customer_name
from [dwh].[dbo].PawnOverdue 
where FromDate = @check_date
and CREATED = FromDate) pod on pod.PawnID = pa.Pawnid
left join [dwh].[dbo].Customer cuss on cuss.CustomerID = pa.CustomerID
where  occ.OpenHour is not null
 """


# In[181]:


# [dwh].[dbo].PawnAssetFile

pawn_infor_for_compliance = pd.read_sql_query(pawn_infor_str, db)

pawn_infor_for_compliance.first_upload_to_open.head(5)

pawn_infor_for_compliance = pawn_infor_for_compliance.drop_duplicates()

pawn_infor_for_compliance.pawnid.count()


# In[182]:



pawn_infor_for_compliance = pawn_infor_for_compliance.merge(shop_data, left_on = 'shopcode', right_on = 'Mã PGD', how = 'left')

pawn_infor_for_compliance = pawn_infor_for_compliance.rename(columns={'Tên PGD': 'shop_name'})

focus_shop_lastest.columns = ['shop_name', 'shop_focus_level']


# In[183]:


shop_check_history.columns


# In[184]:


pawn_infor_for_compliance = pawn_infor_for_compliance.merge(focus_shop_lastest, on = 'shop_name', how = 'left' )

pawn_infor_for_compliance.columns

shop_check_history.columns

shop_check_history = shop_check_history[['shopname', 'shop_nums_checked_day', 'total_num_checked_pawn',
       'nums_day_in_month', 'shop_checked_rate', 'average_nums_checked_pawn']]

shop_check_history.columns = ['shop_name', 'shop_nums_checked_day', 'total_num_checked_pawn',
       'nums_day_in_month', 'shop_checked_rate', 'average_nums_checked_pawn']


# In[185]:


pawn_infor_for_compliance = pawn_infor_for_compliance.merge(shop_check_history, on = 'shop_name', how = 'left')

# pawn_infor_for_compliance = pawn_infor_for_compliance[pawn_infor_for_compliance['nums_shop_checked'] <=2]

pawn_infor_for_compliance['packagecode'] = pawn_infor_for_compliance['packagecode'].fillna('NULL')


# In[186]:


#  Tách bảng score card categorical
score_card_categorical_list = list(score_card_categorical.field_check.unique())

score_card_categorical_list

score_card_categorical['key']=1

dfs = dict(tuple(score_card_categorical.groupby('field_check')))

categorical_score_card_papertype = dfs['papertype']

categorical_score_card_papertype.columns = ['categoryname', 'field_check', 'papertype', 'papertype_risk_score', 'key']

categorical_score_card_papertype = categorical_score_card_papertype[['papertype',  'papertype_risk_score']]

categorical_score_card_open_type = dfs['open_type']

categorical_score_card_open_type

pawn_infor_for_compliance.open_type.unique()

categorical_score_card_open_type.columns = ['categoryname', 'field_check', 'open_type', 'open_type_risk_score', 'key']

categorical_score_card_open_type = categorical_score_card_open_type[['open_type', 'open_type_risk_score']]


# In[187]:


categorical_score_card_cus_type = dfs['cus_type']

categorical_score_card_cus_type.head(5)

categorical_score_card_cus_type.columns = ['categoryname', 'field_check', 'cus_type', 'cus_type_risk_score', 'key']

categorical_score_card_cus_type = categorical_score_card_cus_type[['cus_type', 'cus_type_risk_score']]



categorical_score_card_package_time_range = dfs['package_time_range']

categorical_score_card_package_time_range

categorical_score_card_package_time_range.columns = ['categoryname', 'field_check', 'package_time_range', 'package_time_range_risk_score', 'key']

categorical_score_card_package_time_range = categorical_score_card_package_time_range[['categoryname','package_time_range', 'package_time_range_risk_score']]


# In[188]:


# Tách bảng score card numeric
score_card_numeric['key'] = 1

score_card_numeric_all = score_card_numeric[score_card_numeric['categoryname'] =='All']
score_card_numeric_specific = score_card_numeric[score_card_numeric['categoryname'] !='All']

score_card_numeric_all.field_check.unique()

score_card_numeric_all_dfs = dict(tuple(score_card_numeric_all.groupby('field_check')))

score_card_numeric_all['field_check'].unique()


# In[189]:


score_card_numeric_all_ols_pred_score = score_card_numeric_all_dfs['ols_pred_score']

score_card_numeric_all_ols_pred_score.columns = ['categoryname', 'field_check', 'ols_pred_score_min_value', 'ols_pred_score_max_value', 'ols_pred_score_risk_score',
       'package_name', 'unit', 'key']

score_card_numeric_all_ols_pred_score = score_card_numeric_all_ols_pred_score[['key', 'ols_pred_score_min_value', 'ols_pred_score_max_value', 'ols_pred_score_risk_score']]

score_card_numeric_all_cus_score = score_card_numeric_all_dfs['cus_score']

score_card_numeric_all_cus_score.columns = ['categoryname', 'field_check', 'cus_score_min_value', 'cus_score_max_value', 'cus_score_risk_score',
       'package_name', 'unit', 'key']

score_card_numeric_all_cus_score =score_card_numeric_all_cus_score[['key', 'cus_score_min_value', 'cus_score_max_value', 'cus_score_risk_score']]

score_card_numeric_all_open_hour_extracted = score_card_numeric_all_dfs['open_hour_extracted']

score_card_numeric_all_open_hour_extracted.columns = ['categoryname', 'field_check', 'open_hour_extracted_min_value', 'open_hour_extracted_max_value', 'open_hour_extracted_risk_score',
       'package_name', 'unit', 'key']

score_card_numeric_all_open_hour_extracted = score_card_numeric_all_open_hour_extracted[['key', 'open_hour_extracted_min_value', 'open_hour_extracted_max_value', 'open_hour_extracted_risk_score']]


# In[190]:


score_card_numeric_all_average_nums_checked_pawn = score_card_numeric_all_dfs['average_nums_checked_pawn']

score_card_numeric_all_average_nums_checked_pawn.columns = ['categoryname', 'field_check', 'average_nums_checked_pawn_min_value', 'average_nums_checked_pawn_max_value', 'average_nums_checked_pawn_risk_score',
       'package_name', 'unit', 'key']

score_card_numeric_all_average_nums_checked_pawn = score_card_numeric_all_average_nums_checked_pawn[['key', 'average_nums_checked_pawn_min_value', 'average_nums_checked_pawn_max_value', 'average_nums_checked_pawn_risk_score']]





score_card_numeric_all_num_created_pawn = score_card_numeric_all_dfs['num_created_pawn']

score_card_numeric_all_num_created_pawn.columns = ['categoryname', 'field_check', 'num_created_pawn_min_value', 'num_created_pawn_max_value', 'num_created_pawn_risk_score',
       'package_name', 'unit', 'key']


# In[191]:


# # shop_checked_rate

# score_card_numeric_all_shop_checked_rate = score_card_numeric_all_dfs['shop_checked_rate']

# score_card_numeric_all_shop_checked_rate.columns = ['categoryname', 'field_check', 'shop_checked_rate_min_value', 'shop_checked_rate_max_value', 'shop_checked_rate_risk_score',
#        'package_name', 'unit', 'key']

# score_card_numeric_all_shop_checked_rate = score_card_numeric_all_shop_checked_rate[['key', 'shop_checked_rate_min_value', 'shop_checked_rate_max_value', 'shop_checked_rate_risk_score']]

# average_nums_checked_pawn



score_card_numeric_all_num_created_pawn = score_card_numeric_all_num_created_pawn[['key', 'num_created_pawn_min_value', 'num_created_pawn_max_value', 'num_created_pawn_risk_score']]

score_card_numeric_specific.field_check.unique()

score_card_numeric_specific_dfs = dict(tuple(score_card_numeric_specific.groupby('field_check')))

score_card_numeric_specific['field_check'].unique()

score_card_numeric_specific_ltv_index = score_card_numeric_specific_dfs['ltv_index']

score_card_numeric_specific_ltv_index.head(5)

score_card_numeric_specific_nums_image = score_card_numeric_specific_dfs['nums_image']
score_card_numeric_specific_processing_minute = score_card_numeric_specific_dfs['processing_minute']
score_card_numeric_specific_package_money_range = score_card_numeric_specific_dfs['package_money_range']
score_card_numeric_specific_first_upload_to_open = score_card_numeric_specific_dfs['first_upload_to_open']
score_card_numeric_specific_last_upload_to_open = score_card_numeric_specific_dfs['last_upload_to_open']


# In[192]:


# Xu ly LTV
score_card_numeric_specific_ltv_index.head(5)

score_card_numeric_specific_ltv_index['min_value'] = score_card_numeric_specific_ltv_index['min_value'].replace('',0)

score_card_numeric_specific_ltv_index['max_value'] = score_card_numeric_specific_ltv_index['max_value'].replace('',2)

score_card_numeric_specific_ltv_index.columns = ['categoryname', 'field_check', 'ltv_index_min_value', 'ltv_index_max_value', 'ltv_index_risk_score',
       'package_name', 'unit', 'key']

score_card_numeric_specific_ltv_index = score_card_numeric_specific_ltv_index[['categoryname', 'ltv_index_min_value', 'ltv_index_max_value', 'ltv_index_risk_score']]


# In[193]:


# nums_image

score_card_numeric_specific_nums_image.head(15)

score_card_numeric_specific_nums_image['min_value'] = score_card_numeric_specific_nums_image['min_value'].replace('',0)
score_card_numeric_specific_nums_image['max_value'] = score_card_numeric_specific_nums_image['max_value'].replace('',999)

score_card_numeric_specific_nums_image.columns = ['categoryname', 'field_check', 'nums_image_min_value', 'nums_image_max_value', 'nums_image_risk_score',
       'package_name', 'unit', 'key']

score_card_numeric_specific_nums_image = score_card_numeric_specific_nums_image[['categoryname', 'nums_image_min_value', 'nums_image_max_value', 'nums_image_risk_score']]


# In[194]:


# Xu ly package_money_range

score_card_numeric_specific_package_money_range.head(5)

score_card_numeric_specific_package_money_range['min_value'] = score_card_numeric_specific_package_money_range['min_value'].replace('',0)
score_card_numeric_specific_package_money_range['max_value'] = score_card_numeric_specific_package_money_range['max_value'].replace('',9999)

score_card_numeric_specific_package_money_range.columns = ['categoryname', 'field_check', 'package_money_range_min_value', 'package_money_range_max_value', 'package_money_range_risk_score',
       'package_name', 'unit', 'key']

score_card_numeric_specific_package_money_range = score_card_numeric_specific_package_money_range[['categoryname', 'package_money_range_min_value', 'package_money_range_max_value', 'package_money_range_risk_score']]


# In[195]:


# Xu ly score_card_numeric_specific_first_upload_to_open

score_card_numeric_specific_first_upload_to_open.head(5)

score_card_numeric_specific_first_upload_to_open['min_value'] = score_card_numeric_specific_first_upload_to_open['min_value'].replace('',-99999)
score_card_numeric_specific_first_upload_to_open['max_value'] = score_card_numeric_specific_first_upload_to_open['max_value'].replace('',99999)

score_card_numeric_specific_first_upload_to_open.columns = ['categoryname', 'field_check', 'first_upload_to_open_min_value', 'first_upload_to_open_max_value', 'first_upload_to_open_risk_score',
       'package_name', 'unit', 'key']

score_card_numeric_specific_first_upload_to_open = score_card_numeric_specific_first_upload_to_open[['categoryname', 'first_upload_to_open_min_value', 'first_upload_to_open_max_value', 'first_upload_to_open_risk_score']]

# Xu ly last
score_card_numeric_specific_last_upload_to_open.head(5)

score_card_numeric_specific_last_upload_to_open['min_value'] = score_card_numeric_specific_last_upload_to_open['min_value'].replace('',-99999)
score_card_numeric_specific_last_upload_to_open['max_value'] = score_card_numeric_specific_last_upload_to_open['max_value'].replace('',99999)

score_card_numeric_specific_last_upload_to_open.columns = ['categoryname', 'field_check', 'last_upload_to_open_min_value', 'last_upload_to_open_max_value', 'last_upload_to_open_risk_score',
       'package_name', 'unit', 'key']

score_card_numeric_specific_last_upload_to_open = score_card_numeric_specific_last_upload_to_open[['categoryname', 'last_upload_to_open_min_value', 'last_upload_to_open_max_value', 'last_upload_to_open_risk_score']]


# In[196]:


# Xu ly processing_minute
score_card_numeric_specific_processing_minute.head(5)

score_card_numeric_specific_processing_minute['min_value'] = score_card_numeric_specific_processing_minute['min_value'].replace('',-99999)
score_card_numeric_specific_processing_minute['max_value'] = score_card_numeric_specific_processing_minute['max_value'].replace('',99999)

score_card_numeric_specific_processing_minute['package_name'].unique()

score_card_numeric_specific_processing_minute['package_name'] = score_card_numeric_specific_processing_minute['package_name'].fillna('Vay thường')

score_card_numeric_specific_processing_minute.columns = ['categoryname', 'field_check', 'processing_minute_min_value', 'processing_minute_max_value', 'processing_minute_risk_score',
       'package_name', 'unit', 'key']

score_card_numeric_specific_processing_minute = score_card_numeric_specific_processing_minute[['categoryname',  'package_name', 'processing_minute_min_value', 'processing_minute_max_value', 'processing_minute_risk_score']]

pawn_infor_for_compliance.columns

com_pred_score.columns = ['shop_name', 'ols_pred_score']

pawn_infor_for_compliance['key'] = 1

pawn_infor_for_compliance = pawn_infor_for_compliance.merge(com_pred_score,on = 'shop_name', how = 'left')

pawn_infor_for_compliance.category_name.unique()


# In[197]:


#  Merge catcategorical_score_card_cus_type, categorical_score_card_open_type, categorical_score_card_papertype,categorical_score_card_package_time_range - DKXM

pawn_infor_for_compliance = pawn_infor_for_compliance[(pawn_infor_for_compliance['category_name']!= 'Đăng ký Ô tô') & (pawn_infor_for_compliance['category_name']!= 'Ô tô')]

categorical_score_card_cus_type.head(5)


# In[198]:


pawn_infor_for_compliance.shopcode.count()


# In[ ]:





# In[ ]:





# In[199]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance.merge(categorical_score_card_cus_type, on = 'cus_type', how = 'left')

pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(categorical_score_card_open_type, on = 'open_type', how = 'left')


# In[200]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(categorical_score_card_papertype, on = 'papertype', how = 'left')

pawn_infor_for_compliance_merge.head(5)

categorical_score_card_package_time_range.columns = ['category_name', 'package_time_range', 'package_time_range_risk_score']

pawn_infor_for_compliance_merge.columns

pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(categorical_score_card_package_time_range, on = ['category_name','package_time_range'], how = 'left')

pawn_infor_for_compliance_merge.category_name.unique()

pawn_infor_for_compliance_merge.head(5)


# In[201]:


pawn_infor_for_compliance.shopcode.count()


# In[202]:


# Merge numeric all: score_card_numeric_all_cus_score,score_card_numeric_all_num_created_pawn,score_card_numeric_all_ols_pred_score

score_card_numeric_all_cus_score['cus_score_max_value'] = score_card_numeric_all_cus_score['cus_score_max_value'].replace('', 1000)

pawn_infor_for_compliance_merge['customer_score'] = pawn_infor_for_compliance_merge['customer_score'].fillna(pawn_infor_for_compliance_merge['customer_score'].mean())

pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(score_card_numeric_all_cus_score, on = 'key', how = 'left')

pawn_infor_for_compliance_merge.pawnid.count()


# In[203]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['customer_score']>=pawn_infor_for_compliance_merge['cus_score_min_value']) & (pawn_infor_for_compliance_merge['customer_score']<pawn_infor_for_compliance_merge['cus_score_max_value'])]

pawn_infor_for_compliance_merge.pawnid.count()


# In[204]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(score_card_numeric_all_num_created_pawn, on = 'key', how = 'left')
pawn_infor_for_compliance_merge.pawnid.count()


# In[ ]:





# In[205]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['num_created_pawn']>=pawn_infor_for_compliance_merge['num_created_pawn_min_value']) & (pawn_infor_for_compliance_merge['num_created_pawn']<pawn_infor_for_compliance_merge['num_created_pawn_max_value'])]

pawn_infor_for_compliance_merge.pawnid.count()


# In[206]:


score_card_numeric_all_ols_pred_score['ols_pred_score_min_value'] = score_card_numeric_all_ols_pred_score['ols_pred_score_min_value'].replace('',-1000)

pawn_infor_for_compliance_merge['ols_pred_score'] = pawn_infor_for_compliance_merge['ols_pred_score'].fillna(250)

pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(score_card_numeric_all_ols_pred_score, on = 'key', how = 'left')


# In[207]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['ols_pred_score'] >=pawn_infor_for_compliance_merge['ols_pred_score_min_value']) & (pawn_infor_for_compliance_merge['ols_pred_score'] <pawn_infor_for_compliance_merge['ols_pred_score_max_value']) ]
pawn_infor_for_compliance_merge.pawnid.count()


# In[208]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(score_card_numeric_all_open_hour_extracted, on = 'key', how = 'left')


# In[209]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['open_hour_extracted'] >=pawn_infor_for_compliance_merge['open_hour_extracted_min_value']) & (pawn_infor_for_compliance_merge['open_hour_extracted'] <pawn_infor_for_compliance_merge['open_hour_extracted_max_value']) ]
pawn_infor_for_compliance_merge.pawnid.count()


# In[ ]:





# In[ ]:





# In[210]:


score_card_numeric_all_average_nums_checked_pawn['average_nums_checked_pawn_min_value'] = score_card_numeric_all_average_nums_checked_pawn['average_nums_checked_pawn_min_value'].replace('',0)

score_card_numeric_all_average_nums_checked_pawn['average_nums_checked_pawn_max_value'] = score_card_numeric_all_average_nums_checked_pawn['average_nums_checked_pawn_max_value'].replace('',10)

pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(score_card_numeric_all_average_nums_checked_pawn, on = 'key', how = 'left')

pawn_infor_for_compliance_merge.pawnid.count()


# In[211]:


pawn_infor_for_compliance_merge['average_nums_checked_pawn'] = pawn_infor_for_compliance_merge['average_nums_checked_pawn'].fillna(0)


# In[212]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.drop_duplicates()
pawn_infor_for_compliance_merge.pawnid.count()


# In[213]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['average_nums_checked_pawn'] >= pawn_infor_for_compliance_merge['average_nums_checked_pawn_min_value']) & (pawn_infor_for_compliance_merge['average_nums_checked_pawn'] <pawn_infor_for_compliance_merge['average_nums_checked_pawn_max_value']) ]
pawn_infor_for_compliance_merge.pawnid.count()


# In[ ]:





# In[214]:



# Merge specific:score_card_numeric_specific_first_upload_to_open,score_card_numeric_specific_last_upload_to_open,score_card_numeric_specific_ltv_index,score_card_numeric_specific_nums_image, score_card_numeric_specific_package_money_range,score_card_numeric_specific_processing_minute 

score_card_numeric_specific_first_upload_to_open.columns = ['category_name', 'first_upload_to_open_min_value',
       'first_upload_to_open_max_value', 'first_upload_to_open_risk_score']

score_card_numeric_specific_last_upload_to_open.columns = ['category_name', 'last_upload_to_open_min_value',
       'last_upload_to_open_max_value', 'last_upload_to_open_risk_score']

pawn_infor_for_compliance_merge.columns

pawn_infor_for_compliance_merge.pawnid.count()


# In[215]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(score_card_numeric_specific_first_upload_to_open, on = 'category_name', how = 'left')
pawn_infor_for_compliance_merge.pawnid.count()


# In[ ]:





# In[216]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['first_upload_to_open'] >= pawn_infor_for_compliance_merge['first_upload_to_open_min_value']) & (pawn_infor_for_compliance_merge['first_upload_to_open'] < pawn_infor_for_compliance_merge['first_upload_to_open_max_value']) ]
pawn_infor_for_compliance_merge.pawnid.count()


# In[217]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(score_card_numeric_specific_last_upload_to_open, on = 'category_name', how = 'left')
pawn_infor_for_compliance_merge.pawnid.count()


# In[218]:


# test = pawn_infor_for_compliance_merge[pawn_infor_for_compliance_merge['codeno'] == 'HDCC/346DL2/2011/404']


# In[219]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.drop_duplicates()


# In[220]:


pawn_infor_for_compliance_merge.pawnid.count()


# In[221]:


# test = test.merge(test, on = 'category_name', how = 'left')


# In[222]:


# test


# In[223]:


# pawn_infor_for_compliance_merge.to_excel('test.xlsx')


# In[224]:


# pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['last_upload_to_open'] >= pawn_infor_for_compliance_merge['last_upload_to_open_min_value']) & (pawn_infor_for_compliance_merge['last_upload_to_open'] <pawn_infor_for_compliance_merge['last_upload_to_open_max_value']) ]


# In[225]:



pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['last_upload_to_open'] >= pawn_infor_for_compliance_merge['last_upload_to_open_min_value']) & (pawn_infor_for_compliance_merge['last_upload_to_open'] <pawn_infor_for_compliance_merge['last_upload_to_open_max_value']) ]


# In[226]:


pawn_infor_for_compliance_merge.pawnid.count()


# In[227]:


score_card_numeric_specific_ltv_index.columns = ['category_name', 'ltv_index_min_value', 'ltv_index_max_value','ltv_index_risk_score']

pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(score_card_numeric_specific_ltv_index, on = 'category_name', how = 'left')


# In[228]:



pawn_infor_for_compliance_merge.head(5)


# In[229]:



pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['ltv_index'] >= pawn_infor_for_compliance_merge['ltv_index_min_value']) & (pawn_infor_for_compliance_merge['ltv_index'] < pawn_infor_for_compliance_merge['ltv_index_max_value']) ]
pawn_infor_for_compliance_merge.pawnid.count()


# In[230]:



score_card_numeric_specific_nums_image.columns = ['category_name', 'nums_image_min_value', 'nums_image_max_value',
       'nums_image_risk_score']

pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(score_card_numeric_specific_nums_image, on = 'category_name', how = 'left')


# In[231]:


pawn_infor_for_compliance_merge.pawnid.count()


# In[232]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['nums_image'] >= pawn_infor_for_compliance_merge['nums_image_min_value']) & (pawn_infor_for_compliance_merge['nums_image'] < pawn_infor_for_compliance_merge['nums_image_max_value']) ]
pawn_infor_for_compliance_merge.pawnid.count()


# In[233]:


score_card_numeric_specific_package_money_range.columns = ['category_name', 'package_money_range_min_value',
       'package_money_range_max_value', 'package_money_range_risk_score']

pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(score_card_numeric_specific_package_money_range, on = 'category_name', how = 'left')

pawn_infor_for_compliance_merge.pawnid.count()


# In[234]:


pawn_infor_for_compliance_merge['net_disbur'] = pawn_infor_for_compliance_merge['net_disbur']/1000000


# In[235]:



pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['net_disbur'] >= pawn_infor_for_compliance_merge['package_money_range_min_value']) & (pawn_infor_for_compliance_merge['net_disbur'] <pawn_infor_for_compliance_merge['package_money_range_max_value']) ]

score_card_numeric_specific_processing_minute.columns = ['category_name', 'package_name', 'processing_minute_min_value',
       'processing_minute_max_value', 'processing_minute_risk_score']

pawn_infor_for_compliance_merge.pawnid.count()


# In[236]:


score_card_numeric_specific_processing_minute.columns = ['category_name', 'package_name', 'processing_minute_min_value',
       'processing_minute_max_value', 'processing_minute_risk_score']


# In[237]:


pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge.merge(score_card_numeric_specific_processing_minute, on = ['category_name', 'package_name'], how = 'left')
pawn_infor_for_compliance_merge.pawnid.count()


# In[238]:



pawn_infor_for_compliance_merge = pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['processing_minute'] >= pawn_infor_for_compliance_merge['processing_minute_min_value']) & (pawn_infor_for_compliance_merge['processing_minute'] < pawn_infor_for_compliance_merge['processing_minute_max_value']) ]

pawn_infor_for_compliance_merge.pawnid.count()


# In[239]:


pawn_infor_for_compliance_merge[pawn_infor_for_compliance_merge['category_name'] != 'Đăng ký xe máy']['codeno'].count()


# In[240]:


pawn_infor_for_compliance_merge.category_name.unique()


# In[241]:


compliance_list_title = pawn_infor_for_compliance_merge[pawn_infor_for_compliance_merge['category_name'] == 'Đăng ký xe máy']


# In[242]:


compliance_list_normal_asset = pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['category_name'] != 'Đăng ký xe máy') & (pawn_infor_for_compliance_merge['category_name'] != 'Đăng ký Ô tô') & (pawn_infor_for_compliance_merge['category_name'] != 'Ô tô')]
# pawn_infor_for_compliance_merge[(pawn_infor_for_compliance_merge['category_name'] != 'Đăng ký xe máy') & (pawn_infor_for_compliance_merge['category_name'] != 'Đăng ký Ô tô') & (pawn_infor_for_compliance_merge['category_name'] != 'Ô tô')]


# In[243]:


compliance_list_normal_asset.codeno.count()


# In[244]:


compliance_list_title['packagecode'].unique()

compliance_list_normal_asset.open_type.unique()

compliance_list_title = compliance_list_title.merge(focus_package_list, left_on = 'packagecode', right_on = 'package_code', how = 'left')


# In[245]:


compliance_list_title.codeno.count()


# In[246]:


compliance_list_normal_asset.codeno.count()


# In[247]:


compliance_data_list_raw = compliance_list_title.append(compliance_list_normal_asset)

compliance_data_list_raw['nums_day_in_month'] = compliance_data_list_raw['nums_day_in_month'].fillna(check_day)

compliance_data_list_raw[compliance_data_list_raw['open_type_risk_score'] ==0]

compliance_data_list_raw.columns


# In[248]:


compliance_data_list_raw.codeno.count()


# In[249]:


compliance_data_list_raw_upload = compliance_data_list_raw[['Mã PGD', 'Tỉnh/TP','customer_name', 'category_name', 'codeno', 'complete',
        'cus_score_risk_score',
       'cus_type', 'cus_type_risk_score', 'customer_score', 'extract_date',
        'first_upload',
       'first_upload_to_open',  'first_upload_to_open_risk_score',
       'fromdate', 'key', 'last_upload', 'last_upload_to_open',
       
       'last_upload_to_open_risk_score', 'ltv_index','ltv_index_risk_score', 'net_disbur',
       'num_created_pawn', 'num_created_pawn_risk_score',
       'nums_day_in_month', 'nums_image','nums_image_risk_score', 'ols_pred_score',
       'ols_pred_score_risk_score', 'open_hour_extracted',

       'open_hour_extracted_risk_score', 'open_type', 'open_type_risk_score',
       'openhour', 'package_active_stt', 'package_code', 'package_focus_level',
       'package_from_date', 'package_money_range',
       'package_money_range_risk_score', 'package_name',  'package_time_range', 'package_time_range_risk_score',
       'package_to_date', 'packagecode', 'papertype', 'papertype_risk_score',
       'pawnid', 'processing_minute',  'processing_minute_risk_score',
       'shop_checked_rate', 'shop_focus_level', 'shop_name',
       'shop_nums_checked_day', 'shopcode','shop_checked_rate','total_num_checked_pawn','average_nums_checked_pawn','average_nums_checked_pawn_risk_score']]

compliance_data_list_raw_upload = compliance_data_list_raw_upload.fillna(0)

compliance_data_list_raw_upload['total_score'] = compliance_data_list_raw_upload['cus_score_risk_score']+compliance_data_list_raw_upload['cus_type_risk_score']+compliance_data_list_raw_upload['first_upload_to_open_risk_score']+compliance_data_list_raw_upload['last_upload_to_open_risk_score']+compliance_data_list_raw_upload['ltv_index_risk_score']+compliance_data_list_raw_upload['num_created_pawn_risk_score']+compliance_data_list_raw_upload['nums_image_risk_score']+compliance_data_list_raw_upload['ols_pred_score_risk_score']+compliance_data_list_raw_upload['open_hour_extracted_risk_score']+compliance_data_list_raw_upload['open_type_risk_score']+compliance_data_list_raw_upload['package_money_range_risk_score']+compliance_data_list_raw_upload['package_time_range_risk_score']+compliance_data_list_raw_upload['papertype_risk_score']+compliance_data_list_raw_upload['processing_minute_risk_score']+compliance_data_list_raw_upload['average_nums_checked_pawn_risk_score']
compliance_data_list_raw_upload['extract_date'] = checkdate

compliance_data_list_raw_upload

compliance_data_list_raw_upload.columns


# In[250]:


compliance_data_list_raw_upload = compliance_data_list_raw_upload[[ 'extract_date','customer_name','shopcode', 'shop_name','Tỉnh/TP', 'category_name', 'pawnid', 'codeno', 'complete',
       'cus_score_risk_score', 'cus_type', 'cus_type_risk_score',
       'customer_score',  'first_upload',
       'first_upload_to_open', 'first_upload_to_open_risk_score', 'fromdate',
       'key', 'last_upload', 'last_upload_to_open',
       'last_upload_to_open_risk_score', 'ltv_index', 'ltv_index_risk_score',
       'net_disbur', 'num_created_pawn', 'num_created_pawn_risk_score',
       'nums_day_in_month', 'nums_image', 'nums_image_risk_score',
       'ols_pred_score', 'ols_pred_score_risk_score', 'open_hour_extracted',
       'open_hour_extracted_risk_score', 'open_type', 'open_type_risk_score',
       'openhour', 'package_active_stt', 'package_code', 'package_focus_level',
       'package_from_date', 'package_money_range',
       'package_money_range_risk_score', 'package_name', 'package_time_range',
       'package_time_range_risk_score', 'package_to_date', 'packagecode',
       'papertype', 'papertype_risk_score', 'processing_minute',
       'processing_minute_risk_score',  'shop_focus_level',
       'shop_nums_checked_day',  'shop_checked_rate',
       'total_num_checked_pawn', 'average_nums_checked_pawn',
       'average_nums_checked_pawn_risk_score', 'total_score']]

compliance_data_list_raw_upload.pawnid.count()


# In[251]:


compliance_data_list_raw_upload.head(5)


# In[252]:


# # #Lấy ID và truy cập wb Warning.Control
# control_ssheet_id_raw= '1101WzQvQ897hq4xyjvKHesT4ycx8to7bD9EvLGkcPSQ' 
# control_wb_raw=gc.open_by_key(control_ssheet_id_raw)


# In[253]:


# compliance_data_list_raw_upload.codeno.count()


# In[254]:


# write_data(control_wb_raw,'raw_Tuân thủ','raw_Tuân thủ',compliance_data_list_raw_upload)


# In[255]:


# compliance_data_list_raw_upload['shop_checked_rate'].describe()

# compliance_data_list_raw_upload = compliance_data_list_raw_upload[['extract_date', 'customer_name', 'shopcode', 'shop_name', 'Tỉnh/TP',
#        'category_name', 'pawnid', 'codeno', 'complete', 'cus_score_risk_score',
#        'cus_type', 'cus_type_risk_score', 'customer_score', 'first_upload',
#        'first_upload_to_open', 'first_upload_to_open_risk_score', 'fromdate',
#        'key', 'last_upload', 'last_upload_to_open',
#        'last_upload_to_open_risk_score', 'ltv_index', 'ltv_index_risk_score',
#        'net_disbur', 'num_created_pawn', 'num_created_pawn_risk_score',
#        'nums_day_in_month', 'nums_image', 'nums_image_risk_score',
#        'ols_pred_score', 'ols_pred_score_risk_score', 'open_hour_extracted',
#        'open_hour_extracted_risk_score', 'open_type', 'open_type_risk_score',
#        'openhour', 'package_active_stt', 'package_code', 'package_focus_level',
#        'package_from_date', 'package_money_range',
#        'package_money_range_risk_score', 'package_name', 'package_time_range',
#        'package_time_range_risk_score', 'package_to_date', 'packagecode',
#        'papertype', 'papertype_risk_score', 'processing_minute',
#        'processing_minute_risk_score', 'shop_focus_level',
#        'shop_nums_checked_day', 'shop_checked_rate', 
#        'shop_checked_rate_risk_score','average_nums_checked_pawn', 'total_score']]


# In[256]:


# check_sample
total_sample = 40
title_sample_rate = 0.65
normal_asset_rate = 0.35

title_sample = total_sample*title_sample_rate
normal_asset_sample = total_sample*normal_asset_rate

compliance_data_list_raw_upload.category_name.unique()

title_compliance_data_list_raw_upload = compliance_data_list_raw_upload[compliance_data_list_raw_upload['category_name'] == 'Đăng ký xe máy'].sort_values(by = 'total_score', ascending = False)
normass_compliance_data_list_raw_upload = compliance_data_list_raw_upload[compliance_data_list_raw_upload['category_name'] != 'Đăng ký xe máy'].sort_values(by = 'total_score', ascending = False)

title_check_sample = title_compliance_data_list_raw_upload.head(n = int(title_sample))
normal_asset_check_sample = normass_compliance_data_list_raw_upload.head(n = int(normal_asset_sample))

total_sample_compliance_check = title_check_sample.append(normal_asset_check_sample)


# In[257]:


total_sample_compliance_check.head(6)


# In[258]:


total_sample_compliance_check = total_sample_compliance_check.drop_duplicates()


# In[259]:


# write_data(control_wb,'raw_Tuân thủ','raw_Tuân thủ',compliance_data_list_raw_upload)


# total_sample_compliance_check.columns

# year_month	extract_date	shop_name	shopcode	customer	codeno	category_name	pawn_fraud_group	risk_type

year_month = (today+dt.timedelta(days = -1)).strftime('%Y%m')

total_sample_compliance_check['year_month'] = year_month

total_sample_compliance_check['risk_type'] = 'Tuân thủ'

total_sample_compliance_check['pawn_fraud_group'] = np.nan

total_sample_compliance_check_upload = total_sample_compliance_check[['year_month', 'extract_date', 'shop_name','shopcode', 'customer_name','codeno', 'category_name','pawn_fraud_group', 'risk_type']]


# In[267]:


total_sample_compliance_check_upload.columns = ['year_month',
'extrac_date',
'shopname',
'shopcode',
'cusname',
'codeno',
'cate',
'reason_check',
'risk_type']


# In[268]:


total_sample_compliance_check_upload.codeno.count()


# In[277]:


update_data(control_wb,'Checking',total_sample_compliance_check_upload)


# In[262]:


checkdate


# In[263]:


# Random DKOT để hậu kiểm
pawn_infor_str_car_title = """Declare @check_date date = '"""+checkdate+"""'
  select @check_date extract_date,pod.customer_name customer_name,pa.ShopCode shopcode,pa.packagecode,pa.fromdate, pa.Pawnid, pa.CodeNo,cad.NAME category_name , case when pa.PackageCode like '%NHANH%' then N'Vay nhanh' else N'Vay thường' end package_name,
  pod.PaperType,pod.ltv_index ,(pa.LoanMoneyOrg - pa.InsurranceMoney) net_disbur,
    case when pa.CategoryCode like '%17%' then
		case when (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 3000000 then '0. <=3tr'
		when (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 10000000 then '1. 3-10tr'
		when (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 15000000 then '2. 10-15tr'
		when (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 25000000 then '3. 15-25tr'
		else '4. >25tr' end 
  when pa.CategoryCode like '%15%' then
		case when (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 50000000 then '0. <=50tr'
		when (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 100000000 then '2. 100-200tr'
		when pa.PackageCode is not null and  (pa.LoanMoneyOrg - pa.[InsurranceMoney]) > 200000000 then N'4. >200tr'
		when pa.PackageCode is null and  (pa.LoanMoneyOrg - pa.[InsurranceMoney]) <= 300000000 then '3. 200-300tr'
		else N'4. >300tr' end 
 end package_money_range
	, case when frequency <= 30 then N'1. Kỳ hạn 1 tháng'
  when pa.frequency <= 90 then N'2. Kỳ hạn 3 tháng'
  when pa.frequency <= 180 then N'3. Kỳ hạn 6 tháng'
  when pa.frequency <= 270 then N'4. Kỳ hạn 9 tháng'
  when pa.frequency <= 360 then N'5. Kỳ hạn 12 tháng'
  else N'6. >12 tháng' end package_time_range,
  case when pa.previous_pawn is null then N'Khách mới' else N'Khách cũ' end cus_type,
case when pa.previous_pawn_status is null then N'KH mới vay lần đầu' 
when pa.previous_pawn_status in (77,88,99) then N'KH đang nợ xấu'
when pa.previous_pawn_status = 14 then N'7.Vay thêm'
when pa.previous_pawn_status = 1 and pa.pre_pawn_last_close_day = pa.FromDate then N'0.Đáo hạn'
else (
case when pa.sleep_day>180 then N'5.Mở lại từ 181 ngày'
when pa.sleep_day>90 then N'4.Mở lại từ 91-180 ngày' 
when pa.sleep_day> 60 then N'3.Mở lại từ 61-90 ngày'
when pa.sleep_day> 30 then N'2.Mở lại từ 31-60 ngày'
else N'1.Mở lại từ 1-30 ngày' end ) end open_type,
 pa2.num_created_pawn, csh.CUSTOMER_SCORE, 
   ima.nums_image, trans.complete, occ.OpenHour,  datepart(hour, occ.OpenHour) open_hour_extracted,DATEDIFF(minute, pa.InitTime, trans.complete) processing_minute,
    ima.first_upload, ima.last_upload,DATEDIFF(minute,  occ.OpenHour,ima.first_upload) first_upload_to_open,  DATEDIFF(minute,  occ.OpenHour,ima.last_upload) last_upload_to_open
  from (select *,  LAG (CodeNo) over (PARTITION BY customerid, CategoryCode ORDER BY fromdate asc) previous_pawn,
   LAG (STATUS) over (PARTITION BY customerid ORDER BY fromdate asc) previous_pawn_status,
   Lag(CloseDate) over (PARTITION BY customerid, [CategoryCode] order by fromdate) as pre_pawn_last_close_day,
   DATEDIFF(d, Lag(CloseDate) over (PARTITION BY customerid order by fromdate),Fromdate) sleep_day
  from [dwh].[dbo].pawn 
  where FromDate = @check_date
  and STATUS in (1,14))pa
  left join [dwh].[dbo].ShopDetail sd on sd.code = pa.ShopCode
  left join [dwh].[dbo].W_CATEGORY_D cad on cad.CODE = pa.CategoryCode
  left join [dwh].[dbo].OPEN_CLOSE_CONTRACT_BY_TIME occ on occ.CodeNo = pa.CodeNo
  left join ( select b.CUSTOMERID, b.CUSTOMER_SCORE
  from (select CUSTOMERID, max(DATA_DATE) max_date
  FROM [dwh].[dbo].[CustomerScore_History]
  group by CUSTOMERID) a
  left join [dwh].[dbo].[CustomerScore_History] b on (a.CUSTOMERID = b.CUSTOMERID and b.DATA_DATE = a.max_date)) csh on csh.CUSTOMERID = pa.CustomerID
  left join (
  select FromDate, shopcode, count(pawnid) num_created_pawn /*, case when CategoryCode like '%15%' then 'DKOT'
  when CategoryCode like '%17%' then 'DKXM' else 'SPTT' end categoryname, */
  from [dwh].[dbo].pawn
  where FromDate = @check_date
  and status in (1,14)
  group by FromDate, ShopCode
  ) pa2 on (pa.ShopCode = pa2.ShopCode and pa.Fromdate = pa2.Fromdate)
left join (select paf.PawnID, count(paf.PawnAssetFileID) nums_image, max(paf.created) last_upload, min(paf.created) first_upload
from [dwh].[dbo].PawnAssetFile paf
left join [dwh].[dbo].pawn po on po.Pawnid= paf.PawnID
where FromDate = @check_date
and po.STATUS in (1,14)
group by paf.PawnID) ima on ima.PawnID = pa.pawnid
left join (select PAWN_WID,CONTRACT_NO,min(CREATED)complete from [dwh].[dbo].W_PAWN_TRANSACTION_F 
where action_name like N'Cho vay' 
group by PAWN_WID,CONTRACT_NO) trans on trans.CONTRACT_NO=pa.CodeNo
left join (select pawnid, PaperType,(moneyOrg - InsurranceMoney)/MoneyAppraisal ltv_index, CustomerName customer_name
from [dwh].[dbo].PawnOverdue 
where FromDate = @check_date
and CREATED = FromDate) pod on pod.PawnID = pa.Pawnid
left join [dwh].[dbo].Customer cuss on cuss.CustomerID = pa.CustomerID
where  occ.OpenHour is not null
and pa.categorycode like '%15%' """


# In[264]:


cartitle_random = pd.read_sql_query(pawn_infor_str_car_title, db)

nums_sample = 1

cartitle_random_vay_nhanh = cartitle_random[cartitle_random['package_name'] == 'Vay nhanh'].sort_values(by = 'net_disbur', ascending = False)
cartitle_random_vay_thuong = cartitle_random[cartitle_random['package_name'] != 'Vay nhanh'].sort_values(by = 'net_disbur', ascending = False)

car_title_vay_nhanh_check = cartitle_random_vay_nhanh.head(n = nums_sample)

car_title_vay_thuong_check = cartitle_random_vay_thuong.head(n = nums_sample)

total_car_title_check_sample = car_title_vay_nhanh_check.append(car_title_vay_thuong_check)

total_car_title_check_sample['year_month'] = year_month

total_car_title_check_sample['risk_type'] = 'Hậu kiểm thẩm định ĐKOT'

total_car_title_check_sample['pawn_fraud_group'] = np.nan

total_car_title_check_sample.columns

total_car_title_check_sample = total_car_title_check_sample.merge(shop_data, left_on = 'shopcode', right_on = 'Mã PGD', how = 'left')

total_car_title_check_sample = total_car_title_check_sample.rename(columns={'Tên PGD': 'shop_name'})

total_car_title_check_sample = total_car_title_check_sample[['year_month', 'extract_date', 'shop_name','shopcode', 'customer_name','codeno', 'category_name','pawn_fraud_group', 'risk_type']]


# In[278]:


total_car_title_check_sample.columns = ['year_month',
'extrac_date',
'shopname',
'shopcode',
'cusname',
'codeno',
'cate',
'reason_check',
'risk_type']

total_car_title_check_sample


# In[279]:


update_data(control_wb,'Checking',total_car_title_check_sample)


# In[225]:


total_car_title_check_sample


# In[ ]:





# In[ ]:




