import pandas as pd
from pandas.io import gbq
# from io import StringIO
import pandas_gbq
import flask
from flask import request, jsonify, json, redirect

#Tạo biến môi trường
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:/Users/Hovoh/Downloads/pythonkey.json"
from google.cloud import bigquery

#Tạo client để connect bigquery
bigquery_client = bigquery.Client()
#Kết nối với bìgquery
query_job = bigquery_client.query(
  """
  SELECT
    *
  FROM
    `feature-store-345214.raw_data.raw_data`
  """
)

query = bigquery_client.query(
  """
  SELECT
    *
  FROM
    `feature-store-345214.raw_data.raw_data`
  WHERE is_processed = False
  """
)
df = query_job.to_dataframe()
df_process = query.to_dataframe()
app = flask.Flask(__name__)

def process(data):
  data['Order_Date'] = pd.to_datetime(data['Order_Date'])
  data['Ship_Date'] = pd.to_datetime(data['Ship_Date'])

  data['Order_Date_Month'] = data['Order_Date'].dt.month
  data['Order_Date_Day'] = data['Order_Date'].dt.day
  data['Order_Date_Year'] = data['Order_Date'].dt.year
  data['Ship_Month'] = data['Ship_Date'].dt.month
  data['Ship_Day'] = data['Ship_Date'].dt.day
  data['Ship_Year'] = data['Ship_Date'].dt.year
  data['Time_To_Deliver'] = data['Ship_Day'] - data['Order_Date_Day']
  data.drop(data.loc[data['Time_To_Deliver'] < 0].index, inplace=True)
  data.loc[data['Time_To_Deliver'] <= 2, 'Shipping_Method'] = 'Express delivery'
  data.loc[(data['Time_To_Deliver'] > 2) & (data['Time_To_Deliver'] <= 4), 'Shipping_Method'] = 'Fast delivery'
  data.loc[data['Time_To_Deliver'] > 4, 'Shipping_Method'] = 'Economical delivery'
@app.route("/")
def main():
  #Đọc bảng raw_data
  return flask.render_template("query_result.html", results=query.result())

#xử lý dữ liệu
@app.route("/processing/", methods=["POST"])
def processing_data():
  if df[df['is_processed'] == False].empty:
    return 'No data to process'
  else:
    process(df_process)
    df['is_processed'].replace({False: True}, inplace=True)
    df_process['is_processed'].replace({False: True}, inplace=True)
    #upload data bigquery
    pandas_gbq.to_gbq(df, 'raw_data.raw_data', project_id="feature-store-345214", if_exists = 'replace')
    #insert data into table feature-data
    pandas_gbq.to_gbq(df_process, 'raw_data.feature_data', project_id="feature-store-345214", if_exists = 'append')
    query_result = bigquery_client.query(
      """
      SELECT
        *
      FROM
        `feature-store-345214.raw_data.feature_data`
      """
    )
  return flask.render_template("processed.html", results=query_result.result())


ALLOWED_EXTENSIONS = {'csv'}
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/upload/", methods=['GET', 'POST'])
def upload_file():
  if request.method == 'POST':
      file = request.files['file']
      add_column = pd.read_csv(file.filename)
      add_column = add_column.assign(is_processed=False)
      add_column.to_csv(file.filename, index=False, header=True)
      if file:
        client = bigquery.Client(project="feature-store-345214")
        table_ref = client.dataset("raw_data").table("raw_data")
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1 # ignore the header
        job_config.autodetect = True
        with open(file.filename, "rb") as source_file:
            job = client.load_table_from_file(
              source_file, table_ref, job_config=job_config
            )
          # job is async operation so we have to wait for it to finish
            job.result()
            return 'upload sucess!'

if __name__ == "__main__":
    app.run(port=8080, debug=True, use_reloader=True)