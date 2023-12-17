import json
import os
import boto3


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
                    "content": get_json_file_content(full_filename)
                })

    return json_files


def main():
    json_files = get_json_files_from_folders("./data")
    print(json_files)

if __name__ == "__main__":
    main()
