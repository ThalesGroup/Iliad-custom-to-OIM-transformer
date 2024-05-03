from flask import Flask, render_template, request, redirect, url_for
import os
from os.path import join, dirname, realpath
import  csv

import flask_excel as excel


from meduzot_preprocessing import *

# Configuraton files for Meduzot data decoding :
location_file = "parameters/JF_location_conversion.xlsx"
species_file = "parameters/JF_species_conversion.xlsx"
users_file = "parameters/JF_user_ID.csv"
quantity_file = "parameters/JF_quantity_conversion.csv"
size_file = "parameters/JF_size_conversion.csv"


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
          df_OIM = get_observations_new_format(clean_file, meduzot_occurence, location_file, species_file, users_file, quantity_file, size_file)

          # To uncomment if you want to download the file from browser:
#          out_df = df_OIM.to_csv(None, encoding = "ISO-8859-1")

          df_OIM.to_csv(oim_file)#, encoding = "ISO-8859-1")
          with open(oim_file, "r") as file:
               csv_data = csv.reader(file, delimiter=",")
          
               data = []
               for row in csv_data:
                    data.append(row)

               response = excel.make_response_from_array(data, "csv")
#               response = excel.make_response_from_array([[1, 2], [3, 4]], "csv")

               print("data contents :")
               pprint(data)

               print('\n********\nlength of clean file = ', len(df_clean), "\n********\n")
               print('\n********\nlength of OIM observations = ', len(df_OIM), "\n******")

               pprint(response)
               return response

          # To uncomment if you want to download the file from browser:
          # try:
          #      return send_file(oim_file)#, attachment_filename='my_oim_file.csv', mimetype = "Content-Type: application/csv; charset=ISO-8859-1")
          # except Exception as e:
          #      return str(e)

#      return redirect(url_for('index'))


@app.route("/test", methods=['GET', 'POST'])
def test():
      # get the uploaded file
      uploaded_file = "data/export_for Claire_2023_small.csv"
      if uploaded_file != '':
          clean_file = "temp_file.csv"
          oim_file = "my_oim_file.csv"
          df_clean = clean(uploaded_file)
          df_clean.to_csv(clean_file, encoding = "ISO-8859-1")
          meduzot_occurence = "Jellyfish_in_Israeli_Mediterranean_coast"
          df_OIM = get_observations_new_format(clean_file, meduzot_occurence, location_file, species_file, users_file, quantity_file, size_file)

          df_OIM.to_csv(oim_file)#, encoding = "ISO-8859-1")
          with open(oim_file, "r") as file:
               csv_data = csv.reader(file, delimiter=",")
          
               data = []
               for row in csv_data:
                    data.append(row)

               response = excel.make_response_from_array(data, "csv")
#               response = excel.make_response_from_array([[1, 2], [3, 4]], "csv")

               print("data contents :")
               pprint(data)

               print('\n********\nlength of clean file = ', len(df_clean), "\n********\n")
               print('\n********\nlength of OIM observations = ', len(df_OIM), "\n******")

               pprint(response)
               return response




if (__name__ == "__main__"):
     excel.init_excel(app)
#     app.run(port = 5000)
     app.run()
