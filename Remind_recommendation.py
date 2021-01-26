#!/usr/bin/env python
# coding: utf-8

# In[1]:

from IPython import get_ipython
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
import pygsheets
import json
from google.oauth2 import service_account


# In[6]:


# get_ipython().system('pip install yagmail')


# In[8]:


import yagmail


# In[10]:


scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

with open(r'D:\F88\Python code\Immediate_warning\service_account.json') as source:
    info = json.load(source)
credentials = service_account.Credentials.from_service_account_info(info)

client = pygsheets.authorize(service_account_file=r'D:\F88\Python code\Immediate_warning\service_account.json')


# In[11]:


issue_id='1gGbQZRuevMd9mHndXy7GnfIIDSddp9wd7lE2x7D6wiM'
issue_wb=client.open_by_key(issue_id)


# In[12]:


risk_rec_sheet=issue_wb.worksheet_by_title('risk_recommend')

audit_issue_sheet=issue_wb.worksheet_by_title('audit_issue')

audit_rec_sheet=issue_wb.worksheet_by_title('audit_recommend')

contact_sheet=issue_wb.worksheet_by_title('ref')

contact_data=contact_sheet.get_as_df(start="B1",end="C60")


# In[6]:


# get_as_df(has_header=True, index_column=None, start=None, end=None, numerize=True, empty_value='', value_render=<ValueRenderOption.FORMATTED_VALUE: 'FORMATTED_VALUE'>, **kwargs)


# #### Xử lý Data

# In[13]:


risk_data=risk_rec_sheet.get_as_df(has_header=True,start="A7",empty_value='',value_render='FORMATTED_VALUE')


# In[14]:


risk_data.update_date=pd.to_datetime(risk_data.update_date)
risk_data.recommend_date=pd.to_datetime(risk_data.recommend_date)
risk_data.start_date=pd.to_datetime(risk_data.start_date)


# In[15]:


risk_data.deadline=pd.to_datetime(risk_data.deadline)


# In[16]:


audit_issue_data=audit_issue_sheet.get_as_df(has_header=True,start="A10",empty_value='',value_render='FORMATTED_VALUE')


# In[17]:


audit_issue_data.concluded_date=pd.to_datetime(audit_issue_data.concluded_date)
audit_issue_data.update_date=pd.to_datetime(audit_issue_data.update_date)
audit_issue_data.start_date=pd.to_datetime(audit_issue_data.start_date)
audit_issue_data.deadline=pd.to_datetime(audit_issue_data.deadline)


# In[18]:


audit_rec_data=audit_rec_sheet.get_as_df(has_header=True,start="B9",empty_value="")


# In[19]:


audit_rec_data.recommend_date=pd.to_datetime(audit_rec_data.recommend_date)
audit_rec_data.start_date=pd.to_datetime(audit_rec_data.start_date)
audit_rec_data.update_date=pd.to_datetime(audit_rec_data.update_date)
audit_rec_data.deadline=pd.to_datetime(audit_rec_data.deadline)


# In[20]:


contact_dict=dict(zip(contact_data.assignee, contact_data.contact))


# In[21]:


today=dt.datetime.today()


# In[22]:


audit_issue_data['recommend_date_to_today']=audit_issue_data.concluded_date.apply(lambda x: (x-today).days+1)

audit_issue_data['start_date_to_today']=audit_issue_data.start_date.apply(lambda x: (x-today).days+1)

audit_issue_data['update_date_to_today']=audit_issue_data.update_date.apply(lambda x: (x-today).days+1)

audit_issue_data['deadline_to_today']=audit_issue_data.deadline.apply(lambda x: (x-today).days+1)


# In[23]:


audit_rec_data['recommend_date_to_today']=audit_rec_data.recommend_date.apply(lambda x: (x-today).days+1)

audit_rec_data['start_date_to_today']=audit_rec_data.start_date.apply(lambda x: (x-today).days+1)

audit_rec_data['update_date_to_today']=audit_rec_data.update_date.apply(lambda x: (x-today).days+1)

audit_rec_data['deadline_to_today']=audit_rec_data.deadline.apply(lambda x: (x-today).days+1)


# In[24]:


risk_data['recommend_date_to_today']=risk_data.recommend_date.apply(lambda x: (x-today).days+1)
risk_data['start_date_to_today']=risk_data.start_date.apply(lambda x: (x-today).days+1)
risk_data['update_date_to_today']=risk_data.update_date.apply(lambda x: (x-today).days+1)
risk_data['deadline_to_today']=risk_data.deadline.apply(lambda x: (x-today).days+1)


# In[25]:


# Cảnh báo khuyến nghị chưa chốt kế hoạch với bộ phận trên 2 ngày
risk_data.loc[(risk_data.status.str.contains('Chưa'))&(risk_data.recommend_date_to_today<=-2),'warning_type']=1

#Cảnh báo khuyến nghị Đang thực hiện nhưng chưa cập nhật tiến độ mới nhất trong vòng 6 ngày
risk_data.loc[(risk_data.status.str.contains('Đang'))&(risk_data.update_date_to_today<=-6),'warning_type']=2

#Cảnh báo khuyến nghị Đang thực hiện nhưng đã quá deadline
risk_data.loc[(risk_data.status.str.contains('Đang'))&(risk_data.deadline_to_today<0),'warning_type']=3

# Cảnh báo khuyến nghị Đang thực hiện sắp đến deadline  3 ngày
risk_data.loc[(risk_data.status.str.contains('Đang'))&(risk_data.deadline_to_today<3)&(risk_data.deadline_to_today>=0),'warning_type']=4


# In[26]:


# Cảnh báo khuyến nghị chưa chốt kế hoạch với bộ phận trên 2 ngày
audit_rec_data.loc[(audit_rec_data.status.str.contains('Chưa'))&(audit_rec_data.recommend_date_to_today<=-2),'warning_type']=1

#Cảnh báo khuyến nghị Đang thực hiện nhưng chưa cập nhật tiến độ mới nhất trong vòng 6 ngày
audit_rec_data.loc[(audit_rec_data.status.str.contains('Đang'))&(audit_rec_data.update_date_to_today<=-6),'warning_type']=2

#Cảnh báo khuyến nghị Đang thực hiện nhưng đã quá deadline
audit_rec_data.loc[(audit_rec_data.status.str.contains('Đang'))&(audit_rec_data.deadline_to_today<0),'warning_type']=3

# Cảnh báo khuyến nghị Đang thực hiện sắp đến deadline  3 ngày
audit_rec_data.loc[(audit_rec_data.status.str.contains('Đang'))&(audit_rec_data.deadline_to_today<3)&(audit_rec_data.deadline_to_today>=0),'warning_type']=4


# In[27]:


# Cảnh báo khuyến nghị chưa chốt kế hoạch với bộ phận trên 2 ngày
audit_issue_data.loc[(audit_issue_data.status.str.contains('Chưa'))&(audit_issue_data.recommend_date_to_today<=-2),'warning_type']=1

#Cảnh báo khuyến nghị Đang thực hiện nhưng chưa cập nhật tiến độ mới nhất trong vòng 6 ngày
audit_issue_data.loc[(audit_issue_data.status.str.contains('Đang'))&(audit_issue_data.update_date_to_today<=-6),'warning_type']=2

#Cảnh báo khuyến nghị Đang thực hiện nhưng đã quá deadline
audit_issue_data.loc[(audit_issue_data.status.str.contains('Đang'))&(audit_issue_data.deadline_to_today<0),'warning_type']=3

# Cảnh báo khuyến nghị Đang thực hiện sắp đến deadline  3 ngày
audit_issue_data.loc[(audit_issue_data.status.str.contains('Đang'))&(audit_issue_data.deadline_to_today<3)&(audit_issue_data.deadline_to_today>=0),'warning_type']=4


# #### Soạn Nội Dung Email

# In[39]:


def warning_type_1(assignee,df,issue_col):
    contents = [
    "Dear "+assignee +",",
    df.issue_type+" số "+str(df.no)+str(df['yearmonth'])+" đã "+str(abs(df['recommend_date_to_today']))+" ngày Chưa chốt kế hoạch thực hiện"," ",
     "Bạn cần liên hệ với đại diện "+df['risk_owner']+" thực hiện Chốt kế hoạch sớm nhất có thể",
     "Chi tiết Khuyến nghị",
     df[issue_col].to_frame(name='Nội dung'),
     "Truy cập Link Quản lý Khuyến nghị/Sự vụ sau để cập nhật: ",
    "https://docs.google.com/spreadsheets/d/1gGbQZRuevMd9mHndXy7GnfIIDSddp9wd7lE2x7D6wiM/edit?ts=5f7be58a#gid=1292623467",
    "-------------",
    "Trân trọng"
    ]
    return contents


# In[40]:


def warning_type_2(assignee,df,issue_col):
    contents = [
    "Dear "+assignee +",",
    df.issue_type+" số "+str(df['no'])+str(df['yearmonth'])+" đã "+str(abs(df['update_date_to_today']))+" ngày Chưa có cập nhật mới về tiến độ thực hiện"," ",
     "Bạn cần liên hệ với "+df['risk_owner']+" cập nhật tiến độ thực hiện mới nhất của khuyến nghị này.",
     "Chi tiết Khuyến nghị",
      df[issue_col].to_frame(name='Nội dung'),
      "Truy cập Link Quản lý Khuyến nghị/Sự vụ sau để cập nhật: ",
    "https://docs.google.com/spreadsheets/d/1gGbQZRuevMd9mHndXy7GnfIIDSddp9wd7lE2x7D6wiM/edit?ts=5f7be58a#gid=1292623467",
    "-------------",
    "Trân trọng"
    ]
    return contents


# In[41]:


def warning_type_3(assignee,df,issue_col):
    contents = [
    "Dear "+assignee +",",
    df.issue_type+" số "+str(df['no'])+str(df['yearmonth'])+" đã quá Dealine cam kết hoàn thành "+str(abs(df['deadline_to_today']))+" ngày."," ",
     "Bạn cần liên hệ với "+df['risk_owner']+" cập nhật lại tiến độ thực hiện mới nhất của khuyến nghị này.",
     "Chi tiết Khuyến nghị",
     df[issue_col].to_frame(name='Nội dung'),
    "Truy cập Link Quản lý Khuyến nghị/Sự vụ sau để cập nhật: ",
    "https://docs.google.com/spreadsheets/d/1gGbQZRuevMd9mHndXy7GnfIIDSddp9wd7lE2x7D6wiM/edit?ts=5f7be58a#gid=1292623467",
    "-------------",
    "Trân trọng"
    ]
    return contents


# In[42]:


def warning_type_4(assignee,df,issue_col):
    contents = [
    "Dear "+assignee +",",
    df.issue_type+" số "+str(df['no'])+str(df['yearmonth'])+"  Đang thực hiện còn "+str(abs(df['deadline_to_today']))+" ngày nữa sẽ đến Deadline cam kết."," ",
     "Bạn cần liên hệ với "+df['risk_owner']+" cập nhật lại tiến độ thực hiện mới nhất của khuyến nghị này.",
     "Chi tiết Khuyến nghị",
     df[issue_col].to_frame(name='Nội dung'),
    "Truy cập Link Quản lý Khuyến nghị/Sự vụ sau để cập nhật: ",
    "https://docs.google.com/spreadsheets/d/1gGbQZRuevMd9mHndXy7GnfIIDSddp9wd7lE2x7D6wiM/edit?ts=5f7be58a#gid=1292623467",
    "-------------",
    "Trân trọng"
    ]
    return contents


# #### Lấy Danh Sách Cần Gửi Mail

# In[43]:


risk_list=risk_data[risk_data.warning_type.isna()==False].reset_index()
risk_list['issue_type']='KHUYẾN NGHỊ QTRR'


# In[44]:


audit_rec_list=audit_rec_data[audit_rec_data.warning_type.isna()==False].reset_index()
audit_rec_list['issue_type']='KHUYẾN NGHỊ KTNB'


# In[45]:


audit_issue_list=audit_issue_data[audit_issue_data.warning_type.isna()==False].reset_index()
audit_issue_list['issue_type']='SỰ VỤ'


# #### Thực Hiện Gửi Mail

# In[46]:


yag = yagmail.SMTP(user='tranminhphuong@f88.vn',password= '2314253026Bmpmp',host='smtp.gmail.com')


# In[47]:


def send_mail(df,col_list):
    subject_name="[CẢNH BÁO NỘI BỘ] CẬP NHẬT TIẾN ĐỘ THỰC HIỆN "+df.issue_type+" TẠI "+df.risk_owner.upper()
    assignee_name=df.assignee
    assignee_mail=contact_dict[assignee_name]
    if df.warning_type==1:
        contents_des=warning_type_1(assignee_name,df,col_list)
    elif df.warning_type==2:
        contents_des=warning_type_2(assignee_name,df,col_list)
    elif df.warning_type==3:
        contents_des=warning_type_3(assignee_name,df,col_list)
    elif df.warning_type==4:
        contents_des=warning_type_4(assignee_name,df,col_list)
    if df.warning_type<3:
        yag.send(to=[assignee_mail],subject=subject_name ,contents=contents_des)
    else:
        yag.send(to=[assignee_mail,'nguyentunglam@f88.vn', 'nguyenxuanbinh@f88.vn'],subject=subject_name ,contents=contents_des)


# In[48]:


def risk_rec_remind():
    for index,row in risk_list.iterrows():
        send_mail(row,[5,6,8,10,12])
        print('Đã gửi mail cho '+row.assignee)


# In[49]:


def audit_rec_remind():
    for index,row in audit_rec_list.iterrows():
            send_mail(row,[6,7,8,9,10,13,16,15])
    #         print(row[[6,7,8,9,10,13,16,15]])
            print('Đã gửi mail cho '+row.assignee)


# In[50]:


def audit_issue_remind():
    for index,row in audit_issue_list.iterrows():
            send_mail(row,[4,5,6,9,13,15,12])
    #             print(row[[4,5,6,9,13,15,12]])
            print('Đã gửi mail cho '+row.assignee)


# In[51]:


if risk_list.shape[0]>0:
    risk_rec_remind()
if audit_rec_list.shape[0]>0:
    audit_rec_remind()
if audit_issue_list.shape[0]>0:
    audit_issue_remind()


# In[ ]:




