# -*- coding: utf-8 -*-
"""
Created on Fri Jul  4 15:05:21 2025

@author: VONJan

@ems:cxr_xzat163dotcom || ixytyxiatoutlookdotcom

@conpetence: Administrator

"""

import json
import requests
import sqlite3
import time
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
import datetime
from openpyxl import Workbook
import subprocess
import time



# ========= 配置项 ==========
app_id = ""
app_secret = ""
user_access_token = ""  # 替换为实际的用户访问令牌
APP_TOKEN = ""
TABLE_ID = ""
VIEW_ID = ""  # 如果不使用视图，可以留空或删除此项
FIELD_NAME = "条码"
TIME_FIELD = '日期'
MARK_FIELD = '标记'
DB_PATH = 'D:/anaconda/pyprj/shiyanzhan/test/visits.db'
EXCEL_PATH = 'D:/anaconda/pyprj/shiyanzhan/test/1.xlsx'
SCRIPT_PATH = 'D:/anaconda/pyprj/shiyanzhan/test/true4.py'  # 指定另一个py脚本的路径
backup_path = 'D:/anaconda/pyprj/shiyanzhan/test/backup/'

# ========= 数据库 ==========
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS visits
                 (value TEXT PRIMARY KEY, timestamp TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS visits_log
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT, date TEXT, mark TEXT, log_time TEXT, serial_number TEXT, script TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS script_status
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT, start_time TEXT, end_time TEXT, status TEXT)''')
    conn.commit()
    conn.close()

def insert_or_ignore(value, timestamp):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''DELETE FROM visits''')
    c.execute('''INSERT INTO visits (value, timestamp) VALUES (?, ?)''', (value, timestamp))
    conn.commit()
    conn.close()

def insert_log(value, date, mark, log_time, serial_number, script):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''INSERT INTO visits_log (value, date, mark, log_time, serial_number, script) VALUES (?, ?, ?, ?, ?, ?)''', (value, date, mark, log_time, serial_number, script))
    conn.commit()
    conn.close()

def insert_script_status(value, start_time, end_time, status):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT value FROM script_status WHERE value = ?''', (value,))
    existing_value = c.fetchone()
    if existing_value is None:
        c.execute('''INSERT INTO script_status (value, start_time, end_time, status) VALUES (?, ?, ?, ?)''', (value, start_time, end_time, status))
        conn.commit()
    else:
        c.execute('''UPDATE script_status SET end_time = ?, status = ? WHERE value = ?''', (end_time, status, value))
        conn.commit()
    conn.close()


# ========= 获取 app_token（可选） ==========
def get_feishu_app_access_token(app_id, app_secret):
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    payload = {"app_id": app_id, "app_secret": app_secret}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            result = response.json()
            if result.get("code") == 0:
                print("App/tenant access token 获取成功")
            return result
        else:
            print("获取失败:", response.text)
    except requests.exceptions.RequestException as e:
        print(f"请求异常: {e}")

# ========= 获取出厂编号字段 ==========
def Get_Factory_Code():
    while True:
        try:
            client = lark.Client.builder() \
                .app_id(app_id) \
                .app_secret(app_secret) \
                .log_level(lark.LogLevel.INFO) \
                .build()

            search_req = SearchAppTableRecordRequest.builder() \
                .app_token(APP_TOKEN) \
                .table_id(TABLE_ID) \
                .page_size(50) \
                .request_body(SearchAppTableRecordRequestBody.builder()
                    .field_names([FIELD_NAME])
                    .automatic_fields(False)
                    .view_id(VIEW_ID)
                    .build()) \
                .build()

            search_resp = client.bitable.v1.app_table_record.search(search_req)

            if not search_resp.success():
                print(f"search 查询失败: {search_resp.msg}")
                time.sleep(5)
                continue

            items = search_resp.data.items
            if not items:
                print("无记录")
                time.sleep(5)
                continue

            record_ids = [item.record_id for item in items]
            print(f"获取记录 ID 数量: {len(record_ids)}")
            print(f"记录 ID 列表: {record_ids}")

            batch_req = BatchGetAppTableRecordRequest.builder() \
                .app_token(APP_TOKEN) \
                .table_id(TABLE_ID) \
                .request_body(BatchGetAppTableRecordRequestBody.builder()
                    .record_ids(record_ids)
                    .user_id_type("open_id")
                    .with_shared_url(False)
                    .automatic_fields(True)
                    .build()) \
                .build()

            batch_resp = client.bitable.v1.app_table_record.batch_get(batch_req)

            if not batch_resp.success():
                print(f"batch_get 失败: {batch_resp.msg}")
                time.sleep(5)
                continue

            all_marked = True
            for record in batch_resp.data.records:
                fields = record.fields
                if FIELD_NAME in fields:
                    raw_value = fields[FIELD_NAME]
                    if isinstance(raw_value, list) and len(raw_value) > 0 and isinstance(raw_value[0], dict):
                        value = raw_value[0].get("text", "")
                    else:
                        value = str(raw_value)

                    if TIME_FIELD in fields:
                        raw_time = fields[TIME_FIELD]
                        if isinstance(raw_time, list) and len(raw_time) > 0 and isinstance(raw_time[0], dict):
                            date_value = raw_time[0].get("text", "")
                        else:
                            date_value = str(raw_time)
                    else:
                        date_value = ""

                    if MARK_FIELD in fields:
                        raw_mark = fields[MARK_FIELD]
                        if isinstance(raw_mark, list) and len(raw_mark) > 0 and isinstance(raw_mark[0], dict):
                            mark_value = raw_mark[0].get("text", "")
                        else:
                            mark_value = str(raw_mark)
                    else:
                        mark_value = ""

                    print(f"出厂编号: {value}, 日期: {date_value}, 标记: {mark_value}")

                    if mark_value != "1":
                        all_marked = False
                        insert_or_ignore(value, time.strftime("%Y-%m-%d %H:%M:%S"))
                        write_to_excel(value)
                        Insert_Tag(record.record_id)
                        # 生成序号和序列号
                        current_time = datetime.datetime.now()
                        serial_number = f"{current_time.strftime('%Y%m%d')}{value}{record.record_id}"
                        insert_log(value, date_value, mark_value, current_time.strftime("%Y-%m-%d %H:%M:%S"), serial_number, 'nottrue1.py')

            if all_marked:
                print("所有记录已标记，等待5秒后重新监听")
                time.sleep(5)
            else:
                print("有新记录或未标记记录，继续监听")
                call_another_script(SCRIPT_PATH, value)

        except Exception as e:
            print(f"异常: {e}")
            time.sleep(5)

# ========= 更新标记字段 ==========
def Insert_Tag(record_id):
    client = lark.Client.builder() \
        .app_id(app_id) \
        .app_secret(app_secret) \
        .enable_set_token(True) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()

    request = UpdateAppTableRecordRequest.builder() \
        .app_token(APP_TOKEN) \
        .table_id(TABLE_ID) \
        .record_id(record_id) \
        .request_body(AppTableRecord.builder()
            .fields({"标记": 1.0})
            .build()) \
        .build()

    option = lark.RequestOption.builder().user_access_token(user_access_token).build()
    response = client.bitable.v1.app_table_record.update(request, option)

    if not response.success():
        print(f"更新标记失败: {response.msg}")
    else:
        print("标记更新成功")

# ========= 写入excel ==========
def write_to_excel(value):
    filename = "1.xlsx"
    wb = Workbook()
    ws = wb.active
    ws["A1"] = "出厂编号"
    ws.append([value])
    wb.save(EXCEL_PATH)
    print(f"已写入 Excel 文件 {EXCEL_PATH}")

# ========= 调用另一个Python脚本 ==========
def call_another_script(script_path, value):
    try:
        start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        subprocess.run(['python', script_path, value, '0'], check=True)
        end_time = time.strftime("%Y-%m-%d %H:%M:%S")
        insert_script_status(value, start_time, end_time, '1')
        print(f"已调用脚本: {script_path}")
    except subprocess.CalledProcessError as e:
        print(f"调用脚本失败: {e}")

# ========= 设置开机启动 ==========
def set_startup():
    import winreg as reg
    import os

    # 获取当前脚本的路径
    python_script_path = os.path.abspath(__file__)
    startup_path = r'Software\Microsoft\Windows\CurrentVersion\Run'

    with reg.OpenKey(reg.HKEY_CURRENT_USER, startup_path, 0, reg.KEY_SET_VALUE) as key:
        reg.SetValueEx(key, 'MyPythonScript', 0, reg.REG_SZ, f'python {python_script_path}')

# ========= 执行 ==========
if __name__ == "__main__":
    init_db()
    get_feishu_app_access_token(app_id, app_secret)  # 可选
    Get_Factory_Code()
    subprocess.run(['python', 'backup3.py'])
    #set_startup()
