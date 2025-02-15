# readin.py

import os
import re
import pandas as pd
import geopandas as gpd
import logging
from constant import STATES_AND_TERRITORIES, BDC_US_PROVIDER_FILE_PATTERN, TECH_ABBR_MAPPING, BDC_FILE_PATTERN, TABBLOCK20_FILE_PATTERN

def get_state_info(state_abbr):
    for fips, abbr, name in STATES_AND_TERRITORIES:
        if abbr == state_abbr:
            return fips.zfill(2), abbr, name.replace(' ', '_')  # Ensure FIPS code is zero-padded and spaces are replaced by underscores
    raise ValueError(f"State abbreviation {state_abbr} not found in STATES_AND_TERRITORIES")

def prepare_lookup_tables():
    # Prepare dictionaries for quick lookup
    state_dict = {abbr: (fips, name) for fips, abbr, name in STATES_AND_TERRITORIES}
    return state_dict

def load_holder_mapping(base_dir):
    resources_dir = os.path.join(base_dir, 'USA_FCC-bdc', 'resources')
    if not os.path.exists(resources_dir):
        raise FileNotFoundError(f"Resources directory not found: {resources_dir}")
    
    # Find all files matching the pattern
    files = [f for f in os.listdir(resources_dir) if re.match(BDC_US_PROVIDER_FILE_PATTERN, f)]
    if not files:
        raise FileNotFoundError(f"No files matching pattern {BDC_US_PROVIDER_FILE_PATTERN} found in: {resources_dir}")
    
    # Sort files by the date found in group 1 of the pattern match
    files.sort(key=lambda f: re.search(BDC_US_PROVIDER_FILE_PATTERN, f).group(1), reverse=True)
    
    # Use the most recent file
    most_recent_file = files[0]
    holder_mapping_file = os.path.join(resources_dir, most_recent_file)
    
    holder_mapping_df = pd.read_csv(holder_mapping_file)
    # Convert provider_id to string
    holder_mapping_df['provider_id'] = holder_mapping_df['provider_id'].astype(str)

    holder_mapping = dict(zip(holder_mapping_df['provider_id'], holder_mapping_df['holding_company']))
    return holder_mapping

def check_required_files(base_dir, state_abbr):
    logging.debug(f"Checking required files for state: {state_abbr}")
    fips, abbr, name = get_state_info(state_abbr)
    state_dir = f"{fips}_{abbr}_{name}"
    
    bdc_dir = os.path.join(base_dir, 'USA_FCC-bdc', state_dir)
    tabblock_dir = os.path.join(base_dir, 'USA_Census', state_dir)

    if not os.path.exists(bdc_dir):
        logging.error(f"Required directory not found: {bdc_dir}")
        raise FileNotFoundError(f"Required directory not found: {bdc_dir}")
    
    if not os.path.exists(tabblock_dir):
        logging.error(f"Required directory not found: {tabblock_dir}")
        raise FileNotFoundError(f"Required directory not found: {tabblock_dir}")

    bdc_files = [f for f in os.listdir(bdc_dir) if re.match(BDC_FILE_PATTERN, f)]
    tabblock_files = [f for f in os.listdir(tabblock_dir) if re.match(TABBLOCK20_FILE_PATTERN, f)]

    if not bdc_files:
        logging.error(f"No BDC files found in: {bdc_dir}")
        raise FileNotFoundError(f"No BDC files found in: {bdc_dir}")
    
    if not tabblock_files:
        logging.error(f"No Tabblock20 files found in: {tabblock_dir}")
        raise FileNotFoundError(f"No Tabblock20 files found in: {tabblock_dir}")

    return bdc_files, tabblock_files

def read_file(file_path):
    logging.debug(f"Reading file: {file_path}")
    return gpd.read_file(file_path)

def read_data(base_dir, state_abbr, bdc=False):
    """Read data for a given state.
    
    Args:
        base_dir (str): Base directory path
        state_abbr (str): State abbreviation
        bdc (bool): If True, read BDC files, otherwise read tabblock file
    """
    logging.debug(f"Reading data for state: {state_abbr}, BDC={bdc}")
    bdc_files, tabblock_files = check_required_files(base_dir, state_abbr)
    fips, abbr, name = get_state_info(state_abbr)
    state_dir = f"{fips}_{abbr}_{name}"
    
    if bdc:
        bdc_dir = os.path.join(base_dir, 'USA_FCC-bdc', state_dir)
        bdc_file_paths = [os.path.join(bdc_dir, f) for f in bdc_files]
        logging.debug(f"BDC files to process: {bdc_file_paths}")
        return bdc_file_paths  # Return the list of BDC file paths
    else:
        tabblock_file = os.path.join(base_dir, 'USA_Census', state_dir, f'tl_{fips}_tabblock20.shp')
        logging.debug(f"Reading tabblock file: {tabblock_file}")
        try:
            tabblock_data = gpd.read_file(tabblock_file)
            return tabblock_data
        except Exception as e:
            logging.error(f"Error reading tabblock file {tabblock_file}: {str(e)}")
            raise