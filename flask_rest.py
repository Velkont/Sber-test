import flask
from flask import jsonify
import pandas as pd
import os
import sqlite3
import base64
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
filename = 'testData.xlsx'

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/import/xlsx', methods=['GET'])
def import_from_xlsx():
    df = pd.read_excel(filename)
    df['Rep_dt'] = pd.to_datetime(pd.to_datetime(df['Rep_dt']).dt.strftime('%Y-%m-%d'))
    con = sqlite3.connect('database.db')
    cur = con.cursor()
    if cur.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name='data';''').fetchone() is None:
        cur.execute('''CREATE TABLE data(Rep_dt date, Delta float);''')
    else:
        df.to_sql('data', con=con, if_exists='replace', index=False)
    con.commit()
    con.close()
    return jsonify({"result":0, "resultStr": "OK"})



@app.route('/export/sql/<int:lag_num>', methods=['GET'])
def export_with_sql(lag_num):
    con = sqlite3.connect('database.db')
    cur = con.cursor()
    a_text = "Q1JFQVRFIFZJRVcgZGF0YV92aWV3KFJlcF9kdCwgRGVsdGEsIERlbHRhTGFnKQogICAgQVMgCiAgICBTRUxFQ1QgZGF0YS5SZXBfZHQgIFJlcF9kdCwgZGF0YS5EZWx0YSBEZWx0YSwgZGF0ZShkYXRhLlJlcF9kdCwiLQ=="
    b_text = "IG1vbnRoIikgRGVsdGFMYWcgRlJPTSBkYXRhOw=="
    
    a_base64_bytes = a_text.encode('ascii')
    a_message_bytes = base64.b64decode(a_base64_bytes)
    a_message = a_message_bytes.decode('ascii')

    b_base64_bytes = b_text.encode('ascii')
    b_message_bytes = base64.b64decode(b_base64_bytes)
    b_message = b_message_bytes.decode('ascii')
    logging.debug((a_message+str(lag_num)+b_message))
    cur.execute(a_message+str(lag_num)+b_message)
    logging.debug(cur.execute('''SELECT * from data_view;'''))
    df = pd.read_sql('''SELECT * from data_view;''',con=con)
    logging.debug(df)
    ##CREATE VIEW data_view(Rep_dt, Delta, DeltaLag)
    ##AS 
    ##   SELECT data.Rep_dt  Rep_dt, data.Delta Delta, date(data.Rep_dt,"-2 month") DeltaLag FROM data;
    cur.execute('''DROP VIEW data_view;''')
    con.commit()
    con.close()
    return {"result":df.to_json(), "resultStr": "OK"}

@app.route('/export/pandas/<int:lag_num>', methods=['GET'])
def export_with_pandas(lag_num):
    con = sqlite3.connect('database.db')
    cur = con.cursor()
    df = pd.read_sql('''SELECT * from data;''',con=con)
    df['Rep_dt'] = pd.to_datetime(df['Rep_dt']).dt.strftime('%Y-%m-%d')
    df['DeltaLag'] = pd.to_datetime(df['Rep_dt'])+ pd.DateOffset(months=lag_num)
    logging.debug(df)
    logging.debug(df.to_json())
    con.commit()
    con.close()
    return {"result":df.to_json(), "resultStr": "OK"}



app.run()
