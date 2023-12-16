import io
import os
from zipfile import ZipFile
import requests

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def create_downloads_folder_if_not_exists(folder_path: str = "./downloads"):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


def extract_filename_from_uri(uri: str):
    return uri.split("/")[-1]


def download_zip_file(uri: str) -> ZipFile:
    try:
        response = requests.get(uri)
        return ZipFile(io.BytesIO(response.content))
    except requests.exceptions.RequestException as e:
        print(f"Failed to download the file from the following uri: {uri}")
        return None
    except Exception as e:
        print(f"Failed to convert downloaded file to a zip file: {uri}")
        return None


def main():
    create_downloads_folder_if_not_exists()
    for uri in download_uris:
        zip_file = download_zip_file(uri)
        if zip_file is not None:
            print(f"{uri} - downloaded")
            zip_file.filename = extract_filename_from_uri(uri)
            zip_file.extractall(f"./downloads/")


if __name__ == "__main__":
    main()
