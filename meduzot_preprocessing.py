"""
Created on 11/01/2024

@author: Claire LAUDY


Preprocessing of Meduzot files in order to be compatible with OIM and non redundant


"""

import pandas as pd
import sys
from pprint import pprint
import datetime
import numpy as np



meduzot_columns = ['id','email','user name','date & time','region',
				'distance from shore','stingy water','species','placement',
				'quantity','size','activity','lat','lng','image','comments',
				'distance walked on the beach','group']

export_columns = ['ObsID','IndID','datetime_ori','Location_20_Zones_ID',
		'lat','lng','Distance_from_coast','Activity','Quantity_Rank',
		'Quantity','Size_Rank','Jellies_on_the_beach','Species',
		'Gold_User','Photo','Stinging_Water','Survey_transect','AphiaID',
		'Comments_Heb','Comments_Eng','coordinateUncertaintyInMeters']


def clean(meduzot_file):
	"""
	"""
	df_meduzot = pd.read_csv(meduzot_file, encoding = "ISO-8859-1")
	print('\n********\nlength of meduzot file ', meduzot_file, ' = ', len(df_meduzot), "\n********\n")
	df_merged = df_meduzot.drop_duplicates()

	#pprint(df_merged.columns[0])
	# conversion des dates :
	df_merged['date & time'] = pd.to_datetime(df_merged['date & time'], format='%d/%m/%Y %H:%M')

	df_merged['diff'] = df_merged['date & time'].diff(+1).dt.total_seconds().div(60)
	df_merged.loc[(abs(df_merged['diff'])<=1), 'is_dup_date'] = True
	df_merged["is_dup_data"] = df_merged.duplicated(subset=[df_merged.columns[0], #'id',
				'email','user name','region',
	 			'distance from shore','stingy water','species','placement',
	   			'quantity','size','activity','lat','lng','image','comments',
	   			'distance walked on the beach','group'])
	df_merged = df_merged[~((df_merged["is_dup_date"]) & (df_merged["is_dup_data"]))]

	return df_merged



def get_observations(meduzot_file):
	"""
	"""
	meduzot_df = pd.read_csv(meduzot_file, encoding = "ISO-8859-1")
	observation_df = pd.DataFrame()
	
	pprint(meduzot_df)

	obs_nb = 0

	for index, row in meduzot_df.iterrows():
		str_species = row['species']
		species_list = str_species.split(";")
		if row['quantity'] is np.nan:
			str_quantity = "0"
		else:
			str_quantity = row['quantity']
		quantity_list = str_quantity.split(",")
		print(quantity_list)
		if row['size'] is np.nan:
			str_size = "0"
		else:
			str_size = row['size']
		size_list = str_size.split(",")

		for i, species in enumerate(species_list):
			obs_nb = obs_nb+1
			new_observation = {}
			new_observation["ObsID"] =  obs_nb
			new_observation["IndID"] = row["email"]						# to get from Dori
			new_observation["datetime_ori"] = row["date & time"]		
			new_observation["Location_20_Zones_ID"] = row["region"]			# request translation table
			new_observation["lat"] = row["lat"]
			new_observation["lng"] = row["lng"]
			new_observation["Distance_from_coast"] = row["placement"]		# to check with Dori
			new_observation["Activity"] = row["activity"]
			print(quantity_list)	
			if len(quantity_list)>1 :
				print("several observations ", row["email"])
				new_observation["Quantity_Rank"] = quantity_list[i]				# to get from Dori
			else :
				new_observation["Quantity_Rank"] = quantity_list[0]
			if len(quantity_list)>i :
				new_observation["Quantity"] = quantity_list[i]
			else :
				new_observation["Quantity"] = quantity_list[0]
			if len(size_list)>i :
				new_observation["Size_Rank"] = size_list[i]
			else:
				new_observation["Size_Rank"] = size_list[0]
			new_observation["Jellies_on_the_beach"] = row["placement"]=="0"	# to check with Dori
			new_observation["Species"] = species_list[i]
			new_observation["Gold_User"] = str(0)							# to get from Dori
			new_observation["Photo"] = row["image"]
			new_observation["Stinging_Water"] = row["stingy water"]
			new_observation["Survey_transect"] = ""							# to get from Dori
			new_observation["AphiaID"] = species_list[i]					# to get from Dori
			new_observation["Comments_Heb"] = row["comments"]
			new_observation["Comments_Eng"] = ""							# to get from Roxana ? Dori ?
			new_observation["coordinateUncertaintyInMeters"] = 0			# to get from Dori

			#pprint(new_observation)

			new_observation_df = pd.DataFrame(new_observation, index=[0])
			observation_df = pd.concat([observation_df, new_observation_df], ignore_index = True)
			#observation_df.reset_index()

	#pprint(observation_df)
	return observation_df



def get_observations_new_format(meduzot_file, occurence_string):
	"""
	"""
	meduzot_df = pd.read_csv(meduzot_file, encoding = "ISO-8859-1")
	observation_df = pd.DataFrame()
	
	obs_nb = 0
	cpt_species = 0
	index_observation = ""
	id_column = list(meduzot_df)[1]
	for index, row in meduzot_df.iterrows():
		new_index_observation = row[id_column]
		if index_observation == new_index_observation:
			cpt_species = cpt_species+1
		else:
			cpt_species = 0
		index_observation = new_index_observation

		str_species = row['species']
		if row['quantity'] is np.nan:
			str_quantity = "0"
		else:
			str_quantity = row['quantity']
		quantity_list = str_quantity.split(",")
		if row['size'] is np.nan:
			str_size = "0"
		else:
			str_size = row['size']
		size_list = str_size.split(",")

		obs_nb = obs_nb+1
		new_observation = {}
		new_observation["occurrence"] =  occurence_string
		new_observation["occurrenceID"] =  "".join([occurence_string, str(row[id_column]), "_", str(obs_nb)])	# to get from Dori
		new_observation["eventDate"] = row["date & time"]		
		new_observation["decimalLongitude"] = row["lng"]
		new_observation["decimalLatitude"] = row["lat"]
		new_observation["scientificName"] = str_species							# to get from Dori
		if len(quantity_list)>cpt_species:
			if quantity_list[cpt_species] != "-" and quantity_list[cpt_species] != "0":
				new_observation["occurenceStatus"] = "present"					# rule to get from Dori
		else:
			new_observation["occurenceStatus"] = "absent"
		new_observation["basisOfRecord"] = "HumanObservation"
		new_observation["scientificNameID"] = str_species
		new_observation["recordedBy (Ind ID)"] = row["email"]					# to check with Dori
		new_observation["quantificationMethod"] = row["activity"]				# translation table to get from Dori
		new_observation["organismQuantity"] = ""								# rule to get from Dori
		new_observation["organismQuantityType"] = ""							# to get from Dori
		new_observation["sampleSizeUnit"] = "cm"								# to check with Dori
		if len(size_list)>cpt_species :
			new_observation["sampleSizeValue"] = size_list[cpt_species]
		else:
			new_observation["sampleSizeValue"] = size_list[0]
		new_observation["MachineObservation"] = "-"
		new_observation["eventType"] = "0"										# to get from Dori
		new_observation["coordinateUncertaintyInMeters"] = 0					# to get from Dori
		new_observation["strandedJellyfish"] = row["placement"]=="0"			# to check with Dori
		new_observation["goldUser (accuracy)"] = str(0)							# to get from Dori
		new_observation["stinging_Water"] = row["stingy water"]
		if str(row["distance walked on the beach"]) != "nan":
			new_observation["distanceWalkedinmeters"] = row["distance walked on the beach"]
		else:
			new_observation["distanceWalkedinmeters"] = "-"
		new_observation["obsID"] =  row[id_column]
		new_observation["Location_20_Zones_ID"] = row["region"]					# request translation table
		new_observation["Distance_from_coast"] = row["placement"]				# to check with Dori
		if len(quantity_list)>cpt_species :
			new_observation["Quantity_Reported"] = quantity_list[cpt_species]
		else :
			#new_observation["Quantity_Reported"] = quantity_list[0]
			new_observation["Quantity_Reported"] = "unreported"

		new_observation["Comments_Heb"] = row["comments"]

		#pprint(new_observation)

		new_observation_df = pd.DataFrame(new_observation, index=[0])
		observation_df = pd.concat([observation_df, new_observation_df], ignore_index = True)
		observation_df.reset_index()

	return observation_df


if __name__ == "__main__":
	for arg in sys.argv[1:]:
		print(arg)
	function = sys.argv[1]
	if function == "clean":
		export_file = sys.argv[2]
		clean_file = sys.argv[3]
		df = clean(export_file)
		df.to_csv(clean_file)

	if function == "get_observations":
		clean_file = sys.argv[2]
		oim_file = sys.argv[3]
		df = get_observations(clean_file)
		df.to_csv(oim_file)

	if function == "clean_and_export_to_OIM":
		export_file = sys.argv[2]
		clean_file = "~/Remote_Docs/Documents/projects_data/Iliad/jellyfish_pilot_data/temp_file.csv"#sys.argv[3]
		oim_file = sys.argv[3]
		df_clean = clean(export_file)
		df_clean.to_csv(clean_file, encoding = "ISO-8859-1")
		df_OIM = get_observations(clean_file)
		df_OIM.to_csv(oim_file, encoding = "ISO-8859-1")
		print('\n********\nlength of clean file = ', len(df_clean), "\n********\n")
		print('\n********\nlength of OIM observations = ', len(df_OIM), "\n********\n")

	if function == "clean_and_export_to_OIM_new_format":
		export_file = sys.argv[2]
		clean_file = "~/Remote_Docs/Documents/projects_data/Iliad/jellyfish_pilot_data/temp_file.csv"#sys.argv[3]
		oim_file = sys.argv[3]

		df_clean = clean(export_file)
		df_clean.to_csv(clean_file, encoding = "ISO-8859-1")
		
		meduzot_occurence = "Jellyfish_in_Israeli_Mediterranean_coast"
		df_OIM = get_observations_new_format(clean_file, meduzot_occurence)
		df_OIM.to_csv(oim_file, encoding = "ISO-8859-1")
		
		print('\n********\nlength of clean file = ', len(df_clean), "\n********\n")
		print('\n********\nlength of OIM observations = ', len(df_OIM), "\n********\n")






