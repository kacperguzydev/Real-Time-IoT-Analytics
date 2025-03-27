import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi
from loguru import logger

# Directory to store downloaded files
DOWNLOAD_DIR = "./data"
COMPETITION_NAME = "ashrae-energy-prediction"
# Files to download from the competition
FILES_TO_DOWNLOAD = [
    "train.csv",
    "building_metadata.csv",  # Corrected filename from "building_meta.csv"
    "weather_train.csv"
]

def download_ashrae_files():
    """
    Download the specified files from the ASHRAE – Great Energy Predictor III competition on Kaggle.
    If a downloaded file turns out to be a ZIP archive (even with a .csv extension), it renames it,
    extracts the contents, and then removes the temporary ZIP.
    """
    api = KaggleApi()
    api.authenticate()

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    for filename in FILES_TO_DOWNLOAD:
        logger.info(f"Downloading {filename} from '{COMPETITION_NAME}'...")
        try:
            file_path = api.competition_download_file(
                competition=COMPETITION_NAME,
                file_name=filename,
                path=DOWNLOAD_DIR,
                force=True
            )
        except Exception as e:
            logger.error("Error downloading {}: {}", filename, e)
            continue

        # Sometimes the API returns None or a boolean if the file isn't found.
        if not file_path or isinstance(file_path, bool):
            local_file = os.path.join(DOWNLOAD_DIR, filename)
            local_zip = local_file + ".zip"
            if os.path.exists(local_file):
                file_path = local_file
            elif os.path.exists(local_zip):
                file_path = local_zip
            else:
                logger.error(f"File {filename} could not be located after download. Skipping.")
                continue

        # Check if the file is actually a ZIP archive.
        if zipfile.is_zipfile(file_path):
            new_zip_path = file_path + ".zip"
            os.rename(file_path, new_zip_path)
            logger.info(f"Renamed {file_path} to {new_zip_path} for extraction.")

            with zipfile.ZipFile(new_zip_path, 'r') as zip_ref:
                zip_ref.extractall(DOWNLOAD_DIR)
            logger.info(f"Extracted contents of {new_zip_path} to {DOWNLOAD_DIR}.")

            os.remove(new_zip_path)
            logger.info(f"Removed temporary ZIP file {new_zip_path}.")
        else:
            logger.info(f"{file_path} is not a ZIP archive; no extraction needed.")
def delete_ashrae_files():
    """
    Delete downloaded or extracted files listed in FILES_TO_DOWNLOAD from the DOWNLOAD_DIR.
    """
    for filename in FILES_TO_DOWNLOAD:
        file_path = os.path.join(DOWNLOAD_DIR, filename)
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Deleted file: {file_path}")
            except Exception as e:
                logger.error(f"Error deleting {file_path}: {e}")
        else:
            logger.info(f"File {file_path} does not exist, skipping deletion.")