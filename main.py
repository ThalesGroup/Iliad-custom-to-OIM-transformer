from flask import Flask, render_template, request, redirect, url_for
import os
from os.path import join, dirname, realpath

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
@app.route("/", methods=['POST'])
def uploadFiles():
      # get the uploaded file
      uploaded_file = request.files['file']
      if uploaded_file.filename != '':
          #  file_path = os.path.join(app.config['UPLOAD_FOLDER'], uploaded_file.filename)
          # # set the file path
          #  uploaded_file.save(file_path)
          # # save the file
          clean_file = "~/Remote_Docs/Documents/projects_data/Iliad/jellyfish_pilot_data/temp_file.csv"#sys.argv[3]
#          oim_file = sys.argv[3]
          oim_file = "~/Remote_Docs/Documents/projects_data/Iliad/jellyfish_pilot_data/my_oim_file.csv"
          df_clean = clean(uploaded_file)
          df_clean.to_csv(clean_file, encoding = "ISO-8859-1")
          df_OIM = get_observations(clean_file)
          df_OIM.to_csv(oim_file, encoding = "ISO-8859-1")
          print('\n********\nlength of clean file = ', len(df_clean), "\n********\n")
          print('\n********\nlength of OIM observations = ', len(df_OIM), "\n********\n")

      return redirect(url_for('index'))

if (__name__ == "__main__"):
     app.run(port = 5000)
if (__name__ == "__main__"):
     app.run(port = 5000)