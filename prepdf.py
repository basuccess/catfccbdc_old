import pandas as pd
import zipfile
import os
import re
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

import pandas as pd
import zipfile
import os
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def create_technology_code_df():
    logging.debug("Creating technology code DataFrame")
    # Define the paths
    csv_file_path = 'resources/bdc-Fixed-and-Mobile-Technology-Codes.csv'
    zip_file_path = 'resources/bdc-Fixed-and-Mobile-Technology-Codes.zip'

    # Check if the CSV file exists
    if os.path.exists(csv_file_path):
        logging.debug(f"Reading CSV file: {csv_file_path}")
        # Read the CSV file directly
        df = pd.read_csv(csv_file_path)
    else:
        # If the CSV file does not exist, check if the ZIP file exists
        if os.path.exists(zip_file_path):
            logging.debug(f"Extracting and reading ZIP file: {zip_file_path}")
            # Extract the CSV file from the ZIP file and read it
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall('resources')
                df = pd.read_csv(csv_file_path)
        else:
            logging.error("Neither the CSV file nor the ZIP file was found in the 'resources' directory.")
            raise FileNotFoundError("Neither the CSV file nor the ZIP file was found in the 'resources' directory.")

# Define the abbreviation mapping with consistent length
    abbr_mapping = {
        10: "Copper",   # 6 chars
        40: "Cable",    # 5 chars
        50: "Fiber",    # 5 chars
        60: "GeoSat",   # 6 chars
        61: "NGeoSt",   # 6 chars
        70: "UnlFWA",   # 6 chars
        71: "LicFWA",   # 6 chars
        72: "LBRFWA",   # 6 chars
        0: "Other",     # 5 chars
        300: "3G",      # 2 chars
        400: "4GLTE",   # 5 chars
        500: "5GNR"     # 4 chars
    }

    used_abbr = set(abbr_mapping.values())

    def create_abbr(code, name):
        """Create abbreviated name for technology code"""
        if code in abbr_mapping:
            # Return abbreviation with padding to 7 chars
            return abbr_mapping[code].ljust(7)
        return None

    # Check if 'Code' and 'Name' columns exist before applying the function
    if 'Code' in df.columns and 'Name' in df.columns:
        df['Abbr'] = df.apply(lambda row: create_abbr(row['Code'], row['Name']), axis=1)
    else:
        logging.error("'Code' or 'Name' column not found in the DataFrame")
        raise KeyError("'Code' or 'Name' column not found in the DataFrame")

    logging.debug(f"Technology Code DataFrame columns: {df.columns.tolist()}")
    logging.debug("Technology code DataFrame created successfully")
    return df

def create_provider_df():
    logging.debug("Creating provider DataFrame")
    # Define the regex pattern for provider files
    pattern = r'bdc_us_provider_list_[A-Z]\d{2}_(.*)\.(zip|csv)'

    # Find the latest provider list file
    provider_files = [f for f in os.listdir('resources') if re.match(pattern, f)]
    if not provider_files:
        logging.error("No provider list files found in the 'resources' directory.")
        raise FileNotFoundError("No provider list files found in the 'resources' directory.")

    # Extract dates and find the latest file
    provider_files_dates = [(f, re.search(pattern, f).group(1)) for f in provider_files]
    latest_file = max(provider_files_dates, key=lambda x: datetime.strptime(x[1], '%d%b%Y'))[0]

    # Define the path for the latest provider file
    provider_file_path = os.path.join('resources', latest_file)
    logging.debug(f"Latest provider file found: {provider_file_path}")

    # Check if the provider file is a CSV or ZIP and read it
    if provider_file_path.endswith('.csv'):
        provider_df = pd.read_csv(provider_file_path)
    elif provider_file_path.endswith('.zip'):
        with zipfile.ZipFile(provider_file_path, 'r') as zip_ref:
            zip_ref.extractall('resources')
            extracted_files = zip_ref.namelist()
            provider_csv_file = [f for f in extracted_files if f.endswith('.csv')][0]
            provider_df = pd.read_csv(os.path.join('resources', provider_csv_file))
    else:
        logging.error("Unsupported file format for the provider list.")
        raise ValueError("Unsupported file format for the provider list.")

    logging.debug("Provider DataFrame created successfully")
    return provider_df