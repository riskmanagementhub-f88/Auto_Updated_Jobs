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
                        "uid=dwh;pwd=F88!23456789")
print('Connected to SQL server: DWH')


scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

with open(r'D:\F88\Python code\Immediate_warning\service_account.json') as source:
    info = json.load(source)
credentials = service_account.Credentials.from_service_account_info(info)

client = pygsheets.authorize(service_account_file=r'D:\F88\Python code\Immediate_warning\service_account.json')


wb_id='17ofaATfRVG6kDeCm-twA8EoFQjc7xRaVxrFaEvOLhVs'
sheet=client.open_by_key(wb_id)
wo_id='15r30_u_xE0iUexkUgsbZqcZTy0htmSflCp-uty_1hWA'
wo_wb=client.open_by_key(wo_id)
wo_data=wo_wb.worksheet_by_title('list').get_as_df()
wo_data=wo_data[['write_off_period','codeno']]

wo_data['shorten_code']=wo_data.codeno.apply(lambda x: x[5:])

shop_detail_id='1ZTQE_pfBCAUr-0GSMPJBqtCyZVxX4hdiPuT_RDrdjeQ'
shop_detail_wb=client.open_by_key(shop_detail_id)
shopdetail_data=shop_detail_wb.worksheet_by_title('shop_info').get_as_df()
shopdetail_data=shopdetail_data[['Mã PGD','Tháng KT','Năm KT','Vùng','Tỉnh/TP','Quận/Huyện']]
#Function chuyển sheet thành Data Frame
def to_df(wb_name,sheet_name):
    return wb_name.worksheet_by_title(sheetname).get_as_df()
    
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

def get_data(ext_date):
    balance_chip_str="""
Declare @ext_date date='"""+ext_date+"""'
select po.CREATED,po.yearmonth,po.PawnID,ContractCode,CategoryName,po.ShopName,ShopCode,CurrentMoney/1000000 currentmoney,po.papertype,
case when po.papertype not like N'KT1' then N'KT3' else N'KT1' end as kt,po.FromDate,ToDate,dbo.getyearmonth(po.fromdate) brr_ym,
po.ChipSerial pos_seri,pass.isginno,pass.chipserial sys_seri,
--chip.IMEI sys_chip_seri,pass.*,case when pass.ChipSerial<>chip.IMEI then N'diff_seri' else N'same_seri' end as check_seri_pos_database,
case when po.FromDate>'2020-06-10' then N'brr_from_11.06' else N'brr_before_11.06' end as brr_tick,
case when pass.isginno=1 then N'install_chip' else N'not_chip' end as have_chip,
--chip.DEVICE_STATUS,chip.MESSAGE_NOTIFY,chip.NOTIFY_DT,
tuoino,case when po.packagecode is null then N'Vay thường' else packagecode end as package_type,
case when TuoiNo<1 then N'1.Trong hạn'
when TuoiNo<31 then N'2.1-30'
when TuoiNo<61 then N'3.31-60'
when TuoiNo<91 then N'4.61-90'
when TuoiNo<121 then N'5.91-120'
else N'6.>120' end as bucket,PackageCode,chip_warning.WarningCreated last_warning_date,dbo.getyearmonth(chip_warning.WarningCreated) last_warning_ym,
chip_warning.WarningNumber,chip_warning.NumberDay,chip_warning.WarningContent,chip_warning.Commentcat,chip_warning.CommentContent,chip_warning.commentcreated,
datediff(day,chip_warning.WarningCreated,chip_warning.CommentCreated) last_warning_response_day,chip_warning.ProcessStatus,
chip_warning.Distance,chip_warning.DPD dpd_at_last_warning,
chip_warning_2.WarningCreated first_warning_date,dbo.getYearMonth(chip_warning_2.warningcreated)first_warning_ym,
chip_warning_2.DPD dpd_at_first_warning,chip_warning_2.NumberDay first_warning_numberday,chip_warning_2.CommentContent first_warning_cmt,
case when chip_warning_2.CommentCat is null or chip_warning_2.CommentCat like N'' then N'no_response' else N'responsed' end as first_warning_response,
chip_warning_2.WarningContent first_warning_des,
case when chip_warning.CommentCat like N'' or chip_warning.CommentCat is null then N'no_response' else N'responsed' end as shop_response_warning,
shop.areaid from pawnoverdue po
left join ShopDetail shop on shop.name=po.ShopName
left join (select * from ( select pawnid,chipserial, isginno, ROW_NUMBER()
over (partition by pawnid order by created desc) row_id 
from  dwh.dbo.PawnChip where created<dateadd(day,1,@ext_date))pass1 where row_id=1 ) pass on pass.pawnid=po.pawnid
left join  (select * from (select *,row_number()
 over (partition by codeno  order by warningcreated desc,dpd desc ) row_id 
from vw_W_REPORT_WARNING_GPS_F where warningcreated<dateadd(day,1,@ext_date) ) tab where row_id=1) chip_warning
on chip_warning.codeno=po.ContractCode
left join  (select * from (select *,row_number()
 over (partition by codeno  order by warningcreated asc,dpd desc) row_id 
from vw_W_REPORT_WARNING_GPS_F where warningcreated<dateadd(day,1,@ext_date) ) tab where row_id=1) chip_warning_2
on chip_warning_2.codeno=po.ContractCode
--left join (select ky_ketoan wo_period, hop_dong codeno from w_pawn_write_off_f) wo 
--on wo.codeno=po.contractcode
--left join (select * from
--( select *, ROW_NUMBER()
--over (partition by pawn_wid order by created_dt desc) row_id
 --from W_CHIP_SERIAL_F  ) tab where row_id=1 )
 --chip on chip.PAWN_WID=po.PawnID
where po.CategoryName like N'%Đăng ký%' and po.CREATED=@ext_date

"""
    data=pd.read_sql_query(balance_chip_str,db)
    data['shorten_code']=data.contractcode.apply(lambda x: x[5:])
    data=pd.merge(data,wo_data,on='shorten_code',how='left')
    return data


# In[10]:



ext_date=(dt.date.today()+dt.timedelta(days=-1)).strftime("%Y-%m-%d")


# In[12]:


ext_date


data=get_data(ext_date)


data=pd.merge(data,shopdetail_data,left_on='shopcode',right_on='Mã PGD',how='left')


ext_ym=data.yearmonth.max()


overview_tab=pd.pivot_table(data,index=['have_chip','areaid'],columns=['categoryname','package_type'],values='pawnid',aggfunc='count',fill_value=0,margins=True).reset_index()


overview_warning=pd.pivot_table(data[data.last_warning_ym==ext_ym],
               index=['areaid'],columns=['categoryname','package_type'],
               values='pawnid',aggfunc='count',fill_value=0,margins=True).reset_index()


update_install_chip_car=pd.pivot_table(data[(data.brr_tick=='brr_from_11.06')&(data.categoryname=='Đăng ký Ô tô')],
               index=['areaid','package_type'],
               columns='have_chip',values='pawnid',aggfunc='count',fill_value=0,margins=True).reset_index()


update_install_chip_dkxm=pd.pivot_table(data[(data.brr_tick=='brr_from_11.06')&(data.categoryname=='Đăng ký xe máy')],
               index=['areaid','package_type'],
               columns='have_chip',values='pawnid',aggfunc='count',fill_value=0,margins=True).reset_index()

warning_response=pd.pivot_table(data[(data.last_warning_ym==ext_ym)],index=['bucket','warningcontent'],
               columns=['categoryname','shop_response_warning'],values='pawnid',
               aggfunc='count',fill_value=0,margins=True).reset_index()


warning_cohort=pd.pivot_table(data[data.last_warning_ym==ext_ym],index='warningcontent',
               columns='brr_ym',values='pawnid',aggfunc='count',fill_value=0,margins=True).reset_index()


warning_response_day_static=data[(data.shop_response_warning=='responsed')&(data.last_warning_ym==ext_ym)].groupby('warningcontent')['last_warning_response_day'].describe().reset_index()

warning_response_type=pd.pivot_table(data[(data.shop_response_warning=='responsed')&(data.last_warning_ym==ext_ym)],
              index='warningcontent',columns='commentcat',values='pawnid',aggfunc='count',fill_value=0,margins=True).reset_index()



bucket_have_chip=pd.pivot_table(data,index=['categoryname','have_chip'],columns='bucket',
                                values='pawnid',aggfunc='count',fill_value=0,margins=True).reset_index()

warning_type_bucket=pd.pivot_table(data[data.have_chip=='install_chip'],
                                   index='warningcontent',columns='bucket',values='pawnid',
                                   aggfunc='count',fill_value=0,margins=True).reset_index()


overdue_not_warning_list=data[(data.tuoino>30)&(data.have_chip=='install_chip')&(data.last_warning_date.isna())]



overdue_have_chip=data[(data.have_chip=='install_chip')&(data.tuoino>0)]


overdue_have_chip=overdue_have_chip.fillna("")
dkot_after_1106_no_chip=data.loc[(data.brr_tick=='brr_from_11.06')&(data.have_chip=='not_chip')&(data.categoryname=='Đăng ký Ô tô'),
         ['created','areaid','pawnid', 'contractcode', 'categoryname', 'shopname','shopcode', 'currentmoney', 'fromdate', 'todate', 'brr_ym', 'pos_seri','sys_seri','isginno', 'brr_tick', 'have_chip']]


warning_response_overview=pd.pivot_table(data[(data.last_warning_ym==ext_ym)&(data.shop_response_warning=='responsed')]
               ,index='areaid',columns=['categoryname','package_type'],values='pawnid',aggfunc='count',fill_value=0,margins=True).reset_index()

response_stt_per_region=pd.pivot_table(data[(data.last_warning_ym==ext_ym)]
               ,index=['warningcontent','Vùng'],columns=['categoryname','shop_response_warning'],values=['pawnid','currentmoney'],
               aggfunc={'pawnid':'count','currentmoney':'sum'},fill_value=0,margins=True).reset_index()
warning_overview_per_region=pd.pivot_table(data[(data.last_warning_ym==ext_ym)]
               ,index=['warningcontent'],columns=['Vùng'],values=['pawnid','currentmoney'],
               aggfunc={'pawnid':'count','currentmoney':'sum'},fill_value=0,margins=True).reset_index()

response_warning_shop=pd.pivot_table(data[(data.last_warning_ym==ext_ym)&(data.warningcontent.str.contains('Chip bị mất tín hiệu')==False)]
               ,index=['Vùng','shopname'],columns=['shop_response_warning'],values='pawnid',
               aggfunc='count',fill_value=0,margins=True).reset_index()


response_warning_shop['response_rate']=response_warning_shop['responsed']/response_warning_shop.All

response_warning_shop=response_warning_shop.sort_values(by='All',ascending=False)


ins_chip_period=pd.pivot_table(data,index=['categoryname','brr_tick'],columns=['have_chip'],values=['pawnid','currentmoney'],aggfunc={'pawnid':'count','currentmoney':'sum'},fill_value=0,margins=True).reset_index()


dkxm_warning_bucket_kt=pd.pivot_table(data[(data.categoryname=='Đăng ký xe máy') & (data.last_warning_ym==ext_ym)],index=['Vùng','warningcontent'],columns=['kt','bucket'],values='currentmoney',aggfunc='sum',fill_value=0).reset_index()


warning_content_bucket=pd.pivot_table(data[data.last_warning_ym==ext_ym],index=['warningcontent','categoryname'],columns='bucket',values='currentmoney',aggfunc='sum',margins=True,fill_value=0).reset_index()


# Thống kê số HĐ, dư nợ có cảnh báo mất tín hiệu theo Tỉnh
loss_signal_per_province=pd.pivot_table(data[(data.last_warning_ym==ext_ym)&(data.warningcontent.str.contains('Chip bị mất tín hiệu')==False)],index=['categoryname','shop_response_warning'],
               columns='Vùng',values=['pawnid','currentmoney'],aggfunc={'pawnid':'count','currentmoney':'sum'},fill_value=0).reset_index()


# Nợ quá hạn Cảnh báo Mất tín hiệu
loss_signal_bucket=pd.pivot_table(data[(data.last_warning_ym==ext_ym)&(data.warningcontent.str.contains('Chip bị mất tín hiệu'))],index=['bucket'],
               columns='categoryname',values='currentmoney',aggfunc='sum',fill_value=0).reset_index()



loss_sinal_dkxm_kt=pd.pivot_table(data[(data.last_warning_ym==ext_ym)&(data.warningcontent.str.contains('Chip bị mất tín hiệu'))&(data.categoryname=='Đăng ký xe máy')],
               index=['bucket'],
               columns='kt',values=['pawnid','currentmoney'],aggfunc={'pawnid':'count','currentmoney':'sum'},fill_value=0,margins=True).reset_index()





overdue_have_chip['write_off_period'] = overdue_have_chip['write_off_period'].replace('', 'remove')


overdue_have_chip['write_off_period'].unique()



try:
    overdue_have_chip = overdue_have_chip[overdue_have_chip['write_off_period'] !='remove']
except Exception:
    overdue_have_chip=overdue_have_chip


data_sheet=sheet.worksheet_by_title('install_chip_overdue')
data_sheet.clear()
data_sheet.update_value('A1',"Danh sách HĐ Lắp chip quá hạn theo dư nợ tại ngày "+data.created.max().strftime("%d-%m-%Y"))

data_sheet.set_dataframe(overdue_have_chip,start=(2,1))
webbrowser.open('https://docs.google.com/spreadsheets/d/17ofaATfRVG6kDeCm-twA8EoFQjc7xRaVxrFaEvOLhVs/edit#gid=0')
