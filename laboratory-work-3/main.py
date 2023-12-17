import csv
import json
import os
import boto3


def flatten_json(nested_json):
   flattened_json = {}

   def flatten(x, name=''):
      if type(x) is dict:
         for a in x:
            flatten(x[a], name + a + '_')
      elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
      else:
         flattened_json[name[:-1]] = x

   flatten(nested_json)
   return flattened_json


def get_json_file_content(full_filename: str):
    with open(full_filename) as json_file:
        return json.load(json_file)


def get_json_files_from_folders(folder_path: str):
    json_files = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            full_filename = os.path.join(root, file) 
            if full_filename.endswith(".json"):
                json_files.append({
                    "filename": full_filename,
                    "content": flatten_json(get_json_file_content(full_filename))
                })

    return json_files


def write_json_to_csv(json_files):
    for json_file in json_files:
        json_keys = list(json_file["content"].keys())
        with open(json_file["filename"].replace(".json", ".csv"), 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(json_keys)
            writer.writerow([json_file["content"][json_key] for json_key in json_keys])


def main():
    json_files = get_json_files_from_folders("./data")
    write_json_to_csv(json_files)


if __name__ == "__main__":
    main()
