"""
Created on 11/01/2024

@author: Claire LAUDY


Evaluation of the quality of the automatic preprocessing of meduzot 
files with InSyTo based soft.


"""

import pandas as pd
import sys
from pprint import pprint



columns = ['ObsID','IndID','datetime_ori','Location_20_Zones_ID',
		'lat','lng','Distance_from_coast','Activity','Quantity_Rank',
		'Quantity','Size_Rank','Jellies_on_the_beach','Species',
		'Gold_User','Photo','Stinging_Water','Survey_transect','AphiaID',
		'Comments_Heb','Comments_Eng','coordinateUncertaintyInMeters']


def compare_csv_results(processed_file, clean_file):
	"""
	returns the dataframe containing the rows of processed_file and 
	clean_file that are not in both files.
	"""
	df_processed = pd.read_csv(processed_file, names = columns, encoding = "ISO-8859-1")
	df_clean = pd.read_csv(clean_file, names = columns, encoding = "ISO-8859-1")

	df_compare = df_processed.merge(df_clean, indicator=True, how='outer')
	df_result = df_compare[df_compare['_merge'] != 'both']
	return df_result


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        print(arg)
    function = sys.argv[1]
    export_file = sys.argv[2]
    clean_file = sys.argv[3]
    if function == "compare":
        df = compare_csv_results(export_file, clean_file)
        pprint(df)
