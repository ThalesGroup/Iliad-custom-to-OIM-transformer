from flask import Flask, Response, render_template, request, redirect, url_for, send_file,send_from_directory, make_response, jsonify
import os
from os.path import join, dirname, realpath
import  csv

import flask_excel as excel


from meduzot_preprocessing import *


app = Flask(__name__)

# enable debugging mode
app.config["DEBUG"] = True

# Upload folder
UPLOAD_FOLDER = 'static/files'
app.config['UPLOAD_FOLDER'] =  UPLOAD_FOLDER


# Root URL
@app.route('/')
def index():
     # Set The upload HTML template '\templates\index.html'
    return render_template('index.html')


# Get the uploaded files
@app.route("/", methods=['GET', 'POST'])
def uploadFiles():
      # get the uploaded file
      uploaded_file = request.files['file']
      if uploaded_file.filename != '':
          clean_file = "temp_file.csv"
          oim_file = "my_oim_file.csv"
          df_clean = clean(uploaded_file)
          df_clean.to_csv(clean_file, encoding = "ISO-8859-1")
          meduzot_occurence = "Jellyfish_in_Israeli_Mediterranean_coast"
          df_OIM = get_observations_new_format(clean_file, meduzot_occurence)

          # To uncomment if you want to download the file from browser:
#          out_df = df_OIM.to_csv(None, encoding = "ISO-8859-1")

          df_OIM.to_csv(oim_file)#, encoding = "ISO-8859-1")
          with open(oim_file, "r") as file:
               csv_data = csv.reader(file, delimiter=",")
          
               data = []
               for row in csv_data:
                    data.append(row)

               response = excel.make_response_from_array(data, "csv",
                                          file_name="export_data")

               print('\n********\nlength of clean file = ', len(df_clean), "\n********\n")
               print('\n********\nlength of OIM observations = ', len(df_OIM), "\n******")

               pprint(Response(response))
               return Response(response)

          # To uncomment if you want to download the file from browser:
          # try:
          #      return send_file(oim_file)#, attachment_filename='my_oim_file.csv', mimetype = "Content-Type: application/csv; charset=ISO-8859-1")
          # except Exception as e:
          #      return str(e)

#      return redirect(url_for('index'))



if (__name__ == "__main__"):
     app.run(port = 5000)
if (__name__ == "__main__"):
     app.run(port = 5000)