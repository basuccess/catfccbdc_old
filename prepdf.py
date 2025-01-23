import pandas as pd
import zipfile
import os
import re
from datetime import datetime
import logging
from constants import TECH_ABBR_MAPPING, STATES_AND_TERRITORIES

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def create_technology_code_df(base_dir):
    logging.debug("Creating technology code DataFrame")

    resource_dir = os.path.join(base_dir, 'USA_FCC-bdc', 'resources')
    csv_file_path = os.path.join(resource_dir, 'bdc-Fixed-and-Mobile-Technology-Codes.csv')
    zip_file_path = os.path.join(resource_dir, 'bdc-Fixed-and-Mobile-Technology-Codes.zip')   

    # Log paths for debugging
    logging.debug(f"Looking for files in: {os.path.abspath(resource_dir)}")

    # Check if resources directory exists
    if not os.path.exists(resource_dir):
        logging.error(f"Resources directory not found at: {resource_dir}")
        raise FileNotFoundError(f"Resources directory not found at: {resource_dir}")

    # Check if the CSV file exists
    if os.path.exists(csv_file_path):
        logging.debug(f"Reading CSV file: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
    elif os.path.exists(zip_file_path):
        logging.debug(f"Extracting and reading ZIP file: {zip_file_path}")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            df = pd.read_csv(zip_ref.open('bdc-Fixed-and-Mobile-Technology-Codes.csv'))
    else:
        logging.error(f"Neither CSV nor ZIP file found in: {resource_dir}")
        logging.error(f"Expected files at:\n  {csv_file_path}\n  {zip_file_path}")
        raise FileNotFoundError(f"Required files not found in: {resource_dir}")
    
    # Define the abbreviation mapping with consistent length
    abbr_mapping = TECH_ABBR_MAPPING.copy()

    used_abbr = set(abbr_mapping.values())

   # Process any new codes found in file
    for _, row in df.iterrows():
        code = int(row['Code'])
        if code not in abbr_mapping:
            name = row['Name']
            # Create unique 7-char abbreviation
            abbr = name.split()[0][:7].upper()
            suffix = 1
            while abbr in used_abbr:
                abbr = f"{name[:6]}{suffix}".upper()
                suffix += 1
            abbr_mapping[code] = abbr
            used_abbr.add(abbr)
            logging.info(f"Added new technology code mapping: {code} -> {abbr}")

    # Add abbreviations to DataFrame
    df['Abbr'] = df['Code'].astype(int).map(abbr_mapping)
    return df

def create_provider_df(base_dir):
    logging.debug("Creating provider DataFrame")

    # Define the regex pattern for provider files
    pattern = r'bdc_us_provider_list_[A-Z]\d{2}_(.*)\.(zip|csv)'

    # Define and log full resource directory path
    resource_dir = os.path.join(base_dir, 'USA_FCC-bdc', 'resources')
    logging.info(f"Looking for provider files in: {os.path.abspath(resource_dir)}")
    
    if os.path.exists(resource_dir):
        logging.info(f"Directory contents: {os.listdir(resource_dir)}")
    else:
        logging.error(f"Directory does not exist: {os.path.abspath(resource_dir)}")
        raise FileNotFoundError(f"Resources directory not found at: {os.path.abspath(resource_dir)}")

    # Find the latest provider list file
    provider_files = [f for f in os.listdir(resource_dir) if re.match(pattern, f)]
    if not provider_files:
        logging.error("No provider list files found in the 'resources' directory.")
        raise FileNotFoundError("No provider list files found in the 'resources' directory.")

    # Extract dates and find the latest file
    provider_files_dates = [(f, re.search(pattern, f).group(1)) for f in provider_files]
    latest_file = max(provider_files_dates, key=lambda x: datetime.strptime(x[1], '%d%b%Y'))[0]

    # Define the path for the latest provider file
    provider_file_path = os.path.join(resource_dir, latest_file)
    logging.debug(f"Latest provider file found: {provider_file_path}")

    # Check if the provider file is a CSV or ZIP and read it
    if provider_file_path.endswith('.csv'):
        provider_df = pd.read_csv(provider_file_path)
    elif provider_file_path.endswith('.zip'):
        with zipfile.ZipFile(provider_file_path, 'r') as zip_ref:
            zip_ref.extractall(resource_dir)
            extracted_files = zip_ref.namelist()
            provider_csv_file = [f for f in extracted_files if f.endswith('.csv')][0]
            provider_df = pd.read_csv(os.path.join(resource_dir, provider_csv_file))
    else:
        logging.error("Unsupported file format for the provider list.")
        raise ValueError("Unsupported file format for the provider list.")

    logging.debug("Provider DataFrame created successfully")
    return provider_df