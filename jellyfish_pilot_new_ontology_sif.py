
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 20 2023

@author: Claire LAUDY


Acquisition of graph data from UoH csv file with the new 2023 ontology.


"""

from pysyto.structures import conceptual_graphs as cg
from pysyto.structures import ontology_management as om

import owlready2 as owl
import pandas as pd
import csv
import json
import sys
import os, os.path,subprocess

from googletrans import Translator
from httpcore import SyncHTTPProxy

from pprint import pprint







def get_graphs_from_csv(data_filename, ontology_filename, 
                        out_directory_name = "json_input_graphs_from_Meduzot", 
                        out_filename = "graph"):
    print("\nGetting observation graphs from "+data_filename)
    
    name = data_filename.split('/')[-1].replace(".csv", "")

    #Prepare directory for result files :
    newpath = "".join([out_directory_name, "/"])
    if not os.path.exists(newpath):
        os.makedirs(newpath)
#    else:
#        for f in os.listdir(newpath):
#            print(f)
#            os.remove(f)

    onto = owl.get_ontology(ontology_filename).load()

    with open(data_filename, encoding="cp862") as csvfile:
        #df = csv.reader(csvfile, delimiter=",")
        df = csv.reader(csvfile, delimiter=";")

        translator = Translator()

        graph_list = []
        
        dict_observation_ids = {}
        dict_user_ids = {}

        header_line = 1
        nb_graphs = 1

        for row in df:
            if header_line:
                header_line = 0
            else:
                concepts = []
                relations = []
         
                concept_index = 0
         
                user_id = str(row[1])
                if user_id not in dict_user_ids:
                    concept_person = {}
                    dict_user_ids[user_id] = user_id
                concept_person["type"] = {}
                rdfs_label = "GoldUser" if str(row[13]) == "1" else "User"
                user_class = rdfs_label
                owl_class = onto.search_one(label = rdfs_label)
                concept_person["type"]["label"] = owl_class.iri
                concept_person["marker"] = {}
                concept_person["marker"]["type"] = "OwlEntityMarker"
                concept_person["marker"]["properties"]  = {
                    "dataProperties" : {
                        "IndID" : user_id
                        },
                    "objectProperties" :{}
                }
                concept_person["referent"] = "".join([user_class, user_id])
                concepts.append(concept_person)


               #concept Location (linked to occurance concept):
                concept_location = {}
                concept_location["type"] = {}
                owl_class = onto.search_one(label = "Location")
                concept_location["type"]["label"] = owl_class.iri

                location_ref = "".join(["Location", str(row[3])])
                concept_location["referent"] = location_ref
                concept_location["marker"] = {}
                concept_location["marker"]["type"] = "OwlEntityMarker"
                concept_location["marker"]["properties"]  = {
                    "dataProperties" : {
                        "decimalLatitude" : str(row[4]),
                        "decimalLongitude" : str(row[5]),
                        "locationId" : str(row[3])
                        },
                    "objectProperties" :{
                    }
                }
                concepts.append(concept_location)


                #create the concept observation if not already created
                obs_id = str(row[0])
                if obs_id not in dict_observation_ids:
                    dict_observation_ids[obs_id] = obs_id
                    concept_observation = {}
                    concept_observation["type"] = {}
                    owl_class = onto.search_one(label = "HumanObservation")
                    concept_observation["type"]["label"] = owl_class.iri
                    concept_location["referent"] = obs_id
                    concept_observation["marker"] = {}
                    concept_observation["marker"]["type"] = "OwlEntityMarker"
                    concept_observation["marker"]["properties"]  = {
                        "dataProperties" : {
                            "has_Obs_ID" : str(row[0])
                        },
                        "objectProperties" :{
                            "has_Location" : {
                                "type_label" : "Location",
                                "value" : location_ref
                                }
                        }
                    }
                    concepts.append(concept_observation)

                relation_recorded_by = {
                    "type": {
                        "label": "recorded_by"
                    },
                    "arguments": [obs_id, user_id]
                    }
                relations.append(relation_recorded_by)


                #concept occurance (contains the observatio nof 1 species, 
                #linked to concept observation):
                concept_occurance = {}
                concept_occurance["type"] = {}
                #The type of occurance depends on the quantity of observed JF:
                quantity = str(row[8])
                if quantity == "None":
                    owl_class = onto.search_one(label = "Absence")
                else :
                    owl_class = onto.search_one(label = "quantity")
                concept_occurance["type"]["label"] = owl_class.iri

                #The unique referent for an occurance is built with observation id + species observed:
                ref_occ = dict_observation_ids[obs_id]+str(row[12])
 
                
                concept_measure = {}
                concept_measure["type"] = {}
                owl_class = onto.search_one(label = "MeasurementValue")
                concept_occurance["type"]["label"] = owl_class.iri
                #Referent for measure concept = class label + Species + observation ref:
                measure_ref = "".join(["MeasurementValue", str(row[12]), obs_id])
                concept_measure["referent"] = measure_ref
                concepts.append(concept_measure)

                concept_occurance["referent"] = "".join(["MeasurementValue", obs_id])
                text_value = ""
                translation = ""
                if row[18]:
                    text_value = row[18]
                    translation = row[19]

                concept_occurance["marker"] = {}
                concept_occurance["marker"]["type"] = "OwlEntityMarker"
                concept_occurance["marker"]["properties"]  = {
                    "dataProperties" : {
                        "original_text" : text_value,
                        "translated_text" : translation,
                        "original_language" : "he",
                        "translated_language" : "en"
                        },
                    "objectProperties" :{
                        "has_Location" : {
                            "type_label" : "Location",
                            "value" : location_ref
                            },
                       "has_measurementValue" : {
                            "type_label" : "MeasurementValue",
                            "value" : measure_ref
                            }
                    }
                }
                concepts.append(concept_occurance)

                relation_observed_during = {
                    "type": {
                        "label": "observed_during"
                    },
                    "arguments": [ref_occ, obs_id]
                }
                relations.append(relation_observed_during)


                #type is Sting if Stinging waters (row[12]) is "1" 
                #otherwise we consider it's a Swarm event as jellyfish was spotted.
                if str(row[16]) == "1":
                    concept_event = {}
                    concept_event["type"] = {}
                    rdfs_label = "StingingEvent"
                    owl_class = onto.search_one(label = rdfs_label)
                    concept_event["type"]["label"] = owl_class.iri
                    event_referent = "".join([obs_id, "_", str(event_id)])
                    concept_event["referent"] = event_referent
                    concept_event["marker"] = {}
                    concept_event["marker"]["type"] = "".join(["OwlEntityMarker"])
                    dataProperties = {}
                    #TODO : voir si on peut récupérer directement les IRI dans l'ontologie pour les individus de classe :
                    objectProperties = {}
                    concept_event["marker"]["properties"] = {
                        "dataProperties" : dataProperties,
                        "objectProperties" : objectProperties
                        }
                    concepts.append(concept_event)

                    relation_associated_to = {
                        "type": {
                            "label": "associated_to"
                        },
                        "arguments": [obs_id, event_referent]
                    }
                    relations.append(relation_associated_to)


                if str(row[12]) != "-":
                    concept_species = {}
                    concept_species["type"] = {}
                    ref_species = str(row[12])
                    owl_class = onto.search_one(label = ref_species)
                    concept_species["type"]["label"] = owl_class.iri
                    concept_species["referent"] = ref_species
                    concept_species["marker"] = {}
                    concept_species["marker"]["type"] = "".join(["OwlEntityMarker"])
                    dataProperties = {}
                    #TODO : voir si on peut récupérer directement les IRI dans l'ontologie pour les individus de classe :
                    objectProperties = {}
                    concept_species["marker"]["properties"] = {
                        "dataProperties" : dataProperties,
                        "objectProperties" : objectProperties
                        }
                    concepts.append(concept_species)

                    relation_observed_species = {
                        "type": {
                            "label": "observed_species"
                        },
                        "arguments": [ref_occ, ref_species]
                    }
                    relations.append(relation_observed_species)


                # Build graph
                graph = {
                    "concepts": concepts,
                    "relations": relations,
                }

                graph_list.append(graph)

                # Write
                with open("".join([out_directory_name, "/", out_filename, "_", str(nb_graphs), ".json"]), 'w') as outfile:
                    json.dump(graph, outfile, indent=4, )

                nb_graphs = nb_graphs+1

    return graph_list




def main():
    acquire_from_meduzot_file(data_file_list, ontology_filename, strategy_file, semantic_network_filename)


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        print(arg)
    file_name = sys.argv[2]
    ontology_filename = sys.argv[1]
    function = sys.argv[3]
#    ontology_filename = sys.argv[2]
#    strategy_file = sys.argv[3],
#    semantic_network_filename = sys.argv[4]
#    get_graphs_from_meduzot_file(xlsx_file_name)
    if function == "import":
        get_graphs_from_csv(file_name, ontology_filename)
    elif function == "export":
        export_graphs_as_csv(file_name, ontology_filename)