import csv
import json
import os


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


def write_json_to_csv(json_file):
    json_keys = list(json_file["content"].keys())
    with open(json_file["filename"].replace(".json", ".csv"), 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(json_keys)
        writer.writerow([json_file["content"][json_key] for json_key in json_keys])


def main():
    for root, _, files in os.walk("./data"):
        for file in files:
            full_filename = os.path.join(root, file) 
            if full_filename.endswith(".json"):
                write_json_to_csv({"filename": full_filename, "content": flatten_json(get_json_file_content(full_filename))})


if __name__ == "__main__":
    main()
