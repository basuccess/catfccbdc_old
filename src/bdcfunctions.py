# bdcfunctions.py

import os
import pandas as pd
import geopandas as gpd
import json
import logging
from tqdm import tqdm
from constant import STATES_AND_TERRITORIES, TECH_ABBR_MAPPING, BDC_US_PROVIDER_FILE_PATTERN, \
    BDC_TECH_CODES_FILE_PATTERN, BDC_FILE_PATTERN, TABBLOCK20_FILE_PATTERN
from prep import check_required_files
import psutil  # Import psutil for memory monitoring
import gc  # Import garbage collector
from dask import dataframe as dd
from dask.distributed import Client
from collections import defaultdict
from multiprocessing import cpu_count  # Import cpu_count

# Read BDC data and process in chunks

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

def monitor_memory(threshold=80):
    """Monitor memory usage and log a warning if it exceeds the threshold."""
    memory_usage = psutil.virtual_memory().percent
    if memory_usage > threshold:
        logging.warning(f"Memory usage is high: {memory_usage}%")
        gc.collect()  # Force garbage collection

def get_tech_abbr(tech_code):
    """Get technology abbreviation from technology code with detailed logging."""
    try:
        # Convert tech_code to integer if it's a string
        tech_code = int(tech_code)
        if tech_code not in TECH_ABBR_MAPPING:
            logging.warning(f"Unknown technology code: {tech_code} (type: {type(tech_code)})")
            return "Unknown"
        return TECH_ABBR_MAPPING[tech_code][0]
    except (ValueError, TypeError) as e:
        logging.warning(f"Invalid technology code format: {tech_code} (type: {type(tech_code)})")
        return "Unknown"

def get_holder_name(provider_id, holder_mapping):
    """Get holding company name from provider ID with detailed logging."""
    # Convert provider_id to string if it's not already
    provider_id = str(provider_id)
    holder_name = holder_mapping.get(provider_id)
    if holder_name is None:
        logging.warning(f"Unknown provider_id: {provider_id} (type: {type(provider_id)})")
        # Log the first few entries of holder_mapping for debugging
        sample_mapping = dict(list(holder_mapping.items())[:5])
        logging.debug(f"Sample of holder_mapping: {sample_mapping}")
        holder_name = "Unknown"
    return holder_name