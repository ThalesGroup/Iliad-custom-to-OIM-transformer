
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 11 2023

@author: Claire LAUDY
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




def get_graphs_from_IR(data_filename, onto):
    graph_list = []
    return graph_list



def get_graphs_from_meduzot_file(data_filename, out_filename = "json_input_graphs_form_meduzot.json"):
    print("\nGetting observation graphs from "+data_filename)
    df = pd.read_excel (data_filename)

    name = data_filename.split('/')[-1].replace(".xlsx", "")

    df.fillna('', inplace=True)

    translator = Translator()

    graph_list = []
    
    for index, row in df.iterrows():
        concepts = []
        relations = []
 
        concept_index = 0
 
        concept_person = {}
        concept_person["type"] = "GoldUser" if str(row[19]) == "1" else "Person"
        concept_person["referent"] = "user_"+str(row[14])
        concepts.append(concept_person)


        concept_report = {}
        concept_report["type"] = "App_Report"
        concept_report["referent"] = "".join([name, "_", str(row[14])])
        if row[13]:
            text_value = row[13]
            translation = ""

            #proxy={"https": SyncHTTPProxy((b'http', b'proxy-noauth.s3g-labs.trt', 3128, b''))}
            #translator = Translator(service_urls=['translate.googleapis.com'],proxies=proxy)
            #translation = translator.translate(row[13], src="he", dest='en')
            if row[27]:
                translation = row[27]
            concept_report["marker"] = {}
            concept_report["marker"]["type"] = "TranslatedTextMarker"
            concept_report["marker"]["properties"]  = {
                "original_text" : text_value,
                "translated_text" : translation,
                "original_language" : "he",
                "translated_language" : "en"
            }
        concepts.append(concept_report)

        concept_event = {}
        #type is Sting if no jellyfish were seen (row[6] = "Quantity" = 0 ") and Stinging waters (row[12]) is "1" 
        #otherwise we consider it's a Swarm event as jellyfish was spotted.
        concept_event["type"] = "Sting" if str(row[12]) == "1" and str(row[6]) == "0" else "Person"
        concept_event["referent"] = "".join(["event", "_", name, "_", str(row[14])])
        concept_event["marker"] = {}
        concept_event["marker"]["type"] = "".join(["OWLEntityMarker"])
        dataProperties = {}
        dataProperties["date"] = str(row[2])
        dataProperties["distance_from_coast"] = row[15]
        dataProperties["quantity"] = row[6]
        #TODO : voir si on peut récupérer directement les IRI dans l'ontologie pour les individus de classe :
        objectProperties = {}
        objectProperties["location"] = {"class": "Location", "label": str(row[4])}
        objectProperties["species"] = {"class": "Species", "label":str(row[16])}
        concept_event["marker"]["properties"] = {
            "dataProperties" : dataProperties,
            "objectProperties" : objectProperties
            }
        concepts.append(concept_event)

        concept_app = {}
        concept_app["type"] = {
            "label" : "App_Report"
            }
        concept_app["referent"] = "meduzot_app"
        concept_app["marker"] = {}
        concept_app["marker"]["type"] = "".join(["StringMarker"])
        concept_app["marker"]["properties"] = {
            "str" : "Meduzot App",
            }    
        concepts.append(concept_app)
        
         # Define relations
        relation_has_for_object = {
            "type": {
                "label": "has_for_object"
            },
            "arguments": [concept_event["referent"], concept_report["referent"]]

        }
        relations.append(relation_has_for_object)

        relation_reported_by = {
            "type": {
                "label": "reported_by"
            },
            "arguments": [concept_report["referent"], concept_person["referent"]]

        }
        relations.append(relation_reported_by)

        relation_reported_through = {
            "type": {
                "label": "has_reported_through"
            },
            "arguments": [concept_report["referent"], concept_app["referent"]]

        }
        relations.append(relation_reported_through)

        if not row[5] == "Other":
            concept_activity = {}
            concept_activity["type"] = str(row[5])
            concept_activity["referent"] = "".join([str(row[5]), "_", name, "_", str(row[14])])
            concepts.append(concept_activity)
            relation_has_for_activity = {
                "type": {
                    "label": "has_for_activity"
                },
                "arguments": [concept_person["referent"], concept_activity["referent"]]

            }
            relations.append(relation_has_for_object)


        # Build graph
        graph = {
            "concepts": concepts,
            "relations": relations,
        }
        # print(graph)

        graph_list.append(graph)

        # Write
        with open(out_filename, 'w') as outfile:
            json.dump(graph_list, outfile, indent=4, )

    return graph_list




def get_graphs_from_emily_file(data_filename, ontology_filename, out_directory_name = "json_input_graphs_from_emily", out_filename = "graph"):
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

    with open(data_filename) as csvfile:
        #df = csv.reader(csvfile, delimiter=",")
        df = csv.reader(csvfile, delimiter=";")

        translator = Translator()

        graph_list = []
        
        dict_observation_ids = {}
        dict_user_ids = {}
        event_id = 0

        header_line = 1
        nb_graphs = 1

        for row in df:
            if header_line:
                header_line = 0
            else:
                event_id = event_id+1
                concepts = []
                relations = []
         
                concept_index = 0
         
                user_id = str(row[1])
                if user_id not in dict_user_ids:
                    concept_person = {}
                    dict_user_ids[user_id] = user_id
                concept_person["type"] = {}
                rdfs_label = "GoldUser" if str(row[14]) == "1" else "User"
                user_class = rdfs_label
                owl_class = onto.search_one(label = rdfs_label)
                concept_person["type"]["label"] = owl_class.iri
                ref_user = dict_user_ids[user_id]
                concept_person["marker"] = {}
                concept_person["marker"]["type"] = "StringMarker"
                concept_person["marker"]["properties"]  = {
                    "dataProperties" : {
                        "str" : ref_user
                        },
                    "objectProperties" :{}
                }
                concept_person["referent"] = "".join([user_class, ref_user])
                concepts.append(concept_person)


                obs_id = str(row[0])
                if obs_id not in dict_observation_ids:
                    concept_report = {}
                    concept_report["type"] = {}
                    owl_class = onto.search_one(label = "App_Report")
                    concept_report["type"]["label"] = owl_class.iri
                    dict_observation_ids[obs_id] = obs_id
                ref_obs = dict_observation_ids[obs_id]
                concept_report["referent"] = "".join(["App_Report", ref_obs])
                if row[19]:
                    text_value = row[19]
                    translation = row[20]
                    activity  = row[8]

                    #proxy={"https": SyncHTTPProxy((b'http', b'proxy-noauth.s3g-labs.trt', 3128, b''))}
                    #translator = Translator(service_urls=['translate.googleapis.com'],proxies=proxy)
                    #translation = translator.translate(row[13], src="he", dest='en')
                    concept_report["marker"] = {}
                    concept_report["marker"]["type"] = "OwlEntityMarker"
                    concept_report["marker"]["properties"]  = {
                        "dataProperties" : {
                            "original_text" : text_value,
                            "translated_text" : translation,
                            "original_language" : "he",
                            "translated_language" : "en"
                            },
                        "objectProperties" :{
                            "activity" : {
                                "type_label" : "Activity",
                                "value" : activity
                                }
                        }
                    }
                concepts.append(concept_report)

                concept_event = {}
                #type is Sting if no jellyfish were seen (row[6] = "Quantity" = 0 ") and Stinging waters (row[12]) is "1" 
                #otherwise we consider it's a Swarm event as jellyfish was spotted.
                concept_event["type"] = {}
                rdfs_label = "Sting" if str(row[16]) == "1" and str(row[10]) == "0" else "Swarm"
                owl_class = onto.search_one(label = rdfs_label)
                concept_event["type"]["label"] = owl_class.iri
                event_referent = "".join(["event", "_", name, "_", str(event_id)])
                concept_event["referent"] = event_referent
                concept_event["marker"] = {}
                concept_event["marker"]["type"] = "".join(["OwlEntityMarker"])
                dataProperties = {}
                dataProperties["date"] = str(row[3])
                dataProperties["time"] = str(row[2])
                dataProperties["distance_from_coast"] = row[7]
                dataProperties["quantity"] = row[10]
                dataProperties["quantity_rank"] = row[9]
                dataProperties["size_rank"] = row[11]
                dataProperties["aphia_id"] = row[18]
                dataProperties["jellies_on_beach"] = row[12]
                dataProperties["life_cycle"] = row[22]
                dataProperties["source"] = event_referent
                #TODO : voir si on peut récupérer directement les IRI dans l'ontologie pour les individus de classe :
                objectProperties = {}
                objectProperties["location"] = {"type_label": "Location", "value": str(row[4])}
                objectProperties["species"] = {"type_label": "Species", "value":str(row[13])}
                concept_event["marker"]["properties"] = {
                    "dataProperties" : dataProperties,
                    "objectProperties" : objectProperties
                    }
                concepts.append(concept_event)

                concept_app = {}
                owl_class = onto.search_one(label = "App")
                concept_app["type"] = {
                    "label" : owl_class.iri
                    }
                concept_app["referent"] = "meduzot_app"
                concept_app["marker"] = {}
                concept_app["marker"]["type"] = "".join(["StringMarker"])
                concept_app["marker"]["properties"] = {
                    "str" : "Meduzot App",
                    }    
                concepts.append(concept_app)
                
                # concept_activity = {}
                # rdfs_label  = "Activity"
                # referent = "Activity"
                # if not row[8] == "Other":
                #     if not row[8] == "Unspecified":
                #         if not row[8] == "-":
                #             rdfs_label  = str(row[8])
                #             referent = str(row[8])
                # owl_class = onto.search_one(label = rdfs_label)
                # concept_activity["type"] = {
                #     "label" : owl_class.iri
                #     }
                # concept_activity["marker"] = {}
                # concept_activity["marker"]["type"] = "StringMarker"
                # concept_activity["marker"]["properties"]  = {
                #     "dataProperties" : {
                #         "str" : rdfs_label
                #         },
                #     "objectProperties" :{}
                # }
                # concept_activity["referent"] = referent
                # concepts.append(concept_activity)

                 # Define relations
                relation_has_for_object = {
                    "type": {
                        "label": "has_for_object"
                    },
                    "arguments": ["".join(["App_Report",dict_observation_ids[obs_id]]), concept_event["referent"]]

                }
                relations.append(relation_has_for_object)

                relation_reported_by = {
                    "type": {
                        "label": "reported_by"
                    },
                    "arguments": ["".join(["App_Report",dict_observation_ids[obs_id]]), "".join([user_class, dict_user_ids[user_id]])]

                }
                relations.append(relation_reported_by)

                relation_reported_through = {
                    "type": {
                        "label": "has_reported_through"
                    },
                    "arguments": ["".join(["App_Report",dict_observation_ids[obs_id]]), concept_app["referent"], "".join([user_class, dict_user_ids[user_id]])]

                }
                relations.append(relation_reported_through)

                # relation_has_for_activity = {
                #     "type": {
                #         "label": "has_for_activity"
                #     },
                #     "arguments": [concept_person["referent"], concept_activity["referent"]]

                # }
                # relations.append(relation_has_for_object)


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






def export_graphs_as_csv(csv_file_name,  ontology_filename, json_graphs_directory = "trimmed_events"):
    cwd = os.getcwd()
    graphfiles = ["".join([json_graphs_directory, "/", f]) for f in os.listdir(json_graphs_directory) if f.endswith(".json")]

    print("Loading ontology...")
    onto = owl.get_ontology(ontology_filename).load()

    str_default = "null"

    print("reading graph files...")
    data = []
    for file in graphfiles:
        with open(os.path.join(cwd, file), 'r') as graph_file :
            ObsID = ""
            goldUser = "0"
            IndID = ""
            date = ""
            time = ""
            location = ""
            distance = ""
            quantity = ""
            species = ""
            comments_heb = ""
            comments_eng = ""
            sting = "Unspecified"
            activity = "Other"
            quantity_rank = ""
            size_rank = ""
            life_cycle = ""
            jellies_on_beach = "0"
            aphia_id = ""
            graph = json.load(graph_file)
            concepts = graph.get("concepts")
            for concept in concepts:
                if concept["type"]["label"] == "http://webprotege.stanford.edu/R8XkLeoqVm6s7wqomQEtEw3":  #onto.search_one(label ="GoldUser"): concept of type GoldUser
                    goldUser = "1"
                    IndID = concept["referent"].replace("GoldUser", "")
                elif concept["type"]["label"] == "http://webprotege.stanford.edu/R7nXuuGHLWowNFMhnryOkYm":  # onto.search_one(label = "User"): concept of type User
                    IndID = concept["referent"].replace("User", "")
                elif concept["type"]["label"] == "http://webprotege.stanford.edu/RKbvi7ZxPgQQ8xUiNpJIO1" or concept["type"]["label"] == "http://webprotege.stanford.edu/RCBJK5GjqXK9rwtidtsFpuv": # concept of type Swarm or Sting
                    if concept["type"]["label"] == "http://webprotege.stanford.edu/RCBJK5GjqXK9rwtidtsFpuv":
                        sting = "1"
                    if "marker" in concept:
                        location = concept["marker"]["properties"]["objectProperties"]["location"]["value"]
                        species = concept["marker"]["properties"]["objectProperties"]["species"]["value"]
                        distance = concept["marker"]["properties"]["dataProperties"]["distance_from_coast"]
                        quantity = concept["marker"]["properties"]["dataProperties"]["quantity"]
                        quantity_rank = concept["marker"]["properties"]["dataProperties"]["quantity_rank"]
                        size_rank = concept["marker"]["properties"]["dataProperties"]["size_rank"]
                        aphia_id = concept["marker"]["properties"]["dataProperties"]["aphia_id"]
                        life_cycle = concept["marker"]["properties"]["dataProperties"]["life_cycle"]
                        date = concept["marker"]["properties"]["dataProperties"]["date"]
                        time = concept["marker"]["properties"]["dataProperties"]["time"]
                        jellies_on_beach = concept["marker"]["properties"]["dataProperties"]["jellies_on_beach"]
                elif concept["type"]["label"] == "http://webprotege.stanford.edu/R8J0VkChlddLbiY49WuuP5l": # concept of type App_Report
                    ObsID = concept["referent"].replace("App_Report", "")
                    if "marker" in concept:
                        comments_heb = concept["marker"]["properties"]["dataProperties"]["original_text"]
                        comments_eng = concept["marker"]["properties"]["dataProperties"]["translated_text"]  
                        activity = concept["marker"]["properties"]["objectProperties"]["activity"]["value"]

            data.append({"ObsId": ObsID, "IndID": IndID, 
                        "Time": time, "Date_DMY": date,
                        "Location_20": location, "lat": str_default, "long": str_default,
                        "distance_from_coast": distance,
                        "Activity": activity,
                        "Quantity_Rank": quantity_rank, "Quantity": quantity,
                        "Size_Rank": size_rank, 
                        "Jellies_on_the_beach": jellies_on_beach,
                        "Species": species,
                        "Gold_User": goldUser,
                        "Photo": str_default,
                        "Stinging_Water": sting,
                        "Survey_transect": str_default,
                        "AphiaID": aphia_id,
                        "Comments_Heb": comments_heb, "Comments_Eng": comments_eng,
                        "coordinateUncertaintyInMeters": str_default,
                        "lifeCycle": life_cycle
                        })

    df = pd.DataFrame(data)
    pprint(df)

    #df.to_csv(csv_file_name, delimiter=",")
    df.to_csv(csv_file_name)


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
        get_graphs_from_emily_file(file_name, ontology_filename)
    elif function == "export":
        export_graphs_as_csv(file_name, ontology_filename)