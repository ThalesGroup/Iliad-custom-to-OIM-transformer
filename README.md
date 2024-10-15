
This project provide the preprocessing of Meduzot export files, into the format needed to be input in the OIM integration chain.

# Installation

To use the meduzot preprocessing script:
* Clone this project;
* Create a virtual environment using ```bash python3 -m venv my_venv```
(see documentation on venv [here](https://docs.python.org/3/library/venv.html))
* Activate the virtual environment using ```python3 my_venv/bin/activate``` command
* Install the necassary dependancies for the project: ```python3 -m pip install -r requirements.txt```

You are now ready to use the script to process you Meduzot file.

# Usage

To process your Meduzot file, from the root directory of this project, use the following command line :

```bash
python3 -m meduzot_preprocessing clean_and_export_to_OIM_new_format [PATH_to_meduzot_export_file].csv [PATH_to_result_file].csv
```

For instance : 

```bash
python3 -m meduzot_preprocessing clean_and_export_to_OIM_new_format data/export_for\ Claire_2023_small.csv results/test_export.csv
```