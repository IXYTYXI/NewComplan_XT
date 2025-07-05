# -*- coding: utf-8 -*-
"""
Created on Thu Jul  3 13:19:30 2025

@author: VONJan

@ems:cxr_xzat163dotcom || ixytyxiatoutlookdotcom

@conpetence: Administrator

"""

import sys
import pyodbc
import pandas as pd
import sqlite3
import time
import datetime
import shutil
import os
import schedule

# 获取命令行参数
if len(sys.argv) != 3:
    print("Usage: python GetDataV2.py <value> <status>")
    sys.exit(1)

value = sys.argv[1]
status = sys.argv[2]

# 数据库连接参数（请根据实际情况修改）
server = ''     
database = '' # 例如 'MyDatabase'
username = ''      # 例如 'sa'
password = ''      # 例如 'yourStrong(!)Password'

# Excel 文件路径
input_file = 'D:/anaconda/pyprj/shiyanzhan/test/1.xlsx'
output_file = 'D:/anaconda/pyprj/shiyanzhan/test/2.xlsx'
DB_PATH = 'D:/anaconda/pyprj/shiyanzhan/test/visits.db'
backup_path = 'D:/anaconda/pyprj/shiyanzhan/test/backup/'

# 字段定义
value_fields = [
    '标准高压对低压及地KV',
    '标准低压对高压及地KV',
    '标准低压第一段对高压低压及地KV',
    '标准低压第二段对高压低压及地KV',
    '标准低压第三段对高压低压及地KV',
    '标准低压小组间KV',
    '标准三线圈对高压低压KV'
]

pass_fields = [
    '合格判定高压对低压及地KV',
    '合格判定低压对高压及地KV',
    '合格判定低压第一段对高压低压及地KV',
    '合格判定低压第二段对高压低压及地KV',
    '合格判定低压第三段对高压低压及地KV',
    '合格判定低压小组间KV',
    '合格判定三线圈对高压低压KV'
]

# 读取出厂编号列表
df_input = pd.read_excel(input_file)
if '出厂编号' not in df_input.columns:
    raise ValueError("Excel 中未找到“出厂编号”列，请检查列名。")
id_list = df_input['出厂编号'].dropna().astype(str).tolist()

# 创建数据库连接字符串
conn_str = (
    'DRIVER={ODBC Driver 18 for SQL Server};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
    'TrustServerCertificate=yes;'
)

# 备份数据库
def backup_database():
    if not os.path.exists(backup_path):
        os.makedirs(backup_path)
    backup_file = os.path.join(backup_path, f"visits_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}.db")
    shutil.copy(db_path, backup_file)
    print(f"数据库已备份到 {backup_file}")

schedule.every().day.at("01:30").do(backup_database)

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    all_results = []

    for ID in id_list:
        result = {'出厂编号': ID}
        skip_pass = False  # 控制是否跳过合格字段查询
        standard_sanxian_value = None  # 记录三线圈字段值

        # 查询标准字段
        for field in value_fields:
            sql = f"SELECT [{field}] FROM [dbo].[BCP9_主表] WHERE [出厂编号] = ?"
            cursor.execute(sql, ID)
            row = cursor.fetchone()
            value = row[0] if row else None
            result[field] = value

            # 检查是否是三线圈字段并为空
            #if field == '标准三线圈对高压低压KV' and value is None:
            #    standard_sanxian_value = None  # 明确为空，后续控制使用
            #elif field == '标准三线圈对高压低压KV':
            #    standard_sanxian_value = value  # 记录值，后续控制使用

        # 查询合格字段
        for field in pass_fields:
            # 特别处理三线圈对应字段：如果标准为空，则跳过查询，直接写 None
            #if field == '合格判定三线圈对高压低压KV' and standard_sanxian_value is None:
            #    print(f"{ID} 的 标准三线圈字段为空，合格字段 {field} 直接置空")
            #    result[field] = None
            #    continue

            # 正常合格字段处理
            sql = f"SELECT [{field}] FROM [dbo].[BCP9_主表] WHERE [出厂编号] = ?"
            cursor.execute(sql, ID)
            row = cursor.fetchone()
            value = row[0] if row else None
            result[field] = value

            # 一旦某个合格字段为 None，跳过后续合格字段查询
            #if value is None:
            #    print(f"{ID} 的 {field} 合格值为空，停止后续合格字段查询")
            #    break

        all_results.append(result)

    # 创建结果 DataFrame
    df_results = pd.DataFrame(all_results)

    # 合并到原始数据（根据出厂编号对齐）
    df_final = pd.merge(df_input, df_results, on='出厂编号', how='left')

    # 写入原始 Excel 文件（覆盖）
    df_final.to_excel(output_file, index=False)
    print(f"所有出厂编号查询完成，结果已写入 {output_file}")

    # 更新状态
    if status == '0':
        conn = sqlite3.connect('D:/anaconda/pyprj/shiyanzhan/test/visits.db')
        c = conn.cursor()
        c.execute('''UPDATE script_status SET end_time = ?, status = ? WHERE value = ?''', (time.strftime("%Y-%m-%d %H:%M:%S"), '1', value))
        conn.commit()
        conn.close()

    # 写入日志数据库
    conn = sqlite3.connect('D:/anaconda/pyprj/shiyanzhan/test/visits.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS log_db
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT, timestamp TEXT)''')
    for result in all_results:
        c.execute('''INSERT INTO log_db (value, timestamp) VALUES (?, ?)''', (result['出厂编号'], time.strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()
    conn.close()

    # 将数据写入数据库
    conn = sqlite3.connect('D:/anaconda/pyprj/shiyanzhan/test/visits.db')
    c = conn.cursor()
    # 获取当前时间（精确到毫秒）
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    # 遍历 df_final 的每一行，将数据写入数据库
    for index, row in df_final.iterrows():
        value = row['出厂编号']  # 假设 '出厂编号' 是出厂编号的列名
        c.execute('''INSERT INTO excel_data_log (value, timestamp) VALUES (?, ?)''', (value, current_time))

    conn.commit()
    conn.close()



except Exception as e:
    print("查询出错：", e)

finally:
    if 'conn' in locals():
        conn.close()
