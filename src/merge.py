# merge.py

import pandas as pd
import geopandas as gpd
import json
import logging
from tqdm import tqdm
from constant import TECH_ABBR_MAPPING
from prepdata import load_holder_mapping

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

def process_bdc_file(file_path, holder_mapping):
    logging.debug(f"Reading BDC file: {file_path}")
    df = gpd.read_file(file_path)

    # Log the data types of the provider_id column
    logging.debug(f"provider_id dtype: {df['provider_id'].dtype}")
    logging.debug(f"Sample provider_ids: {df['provider_id'].head()}")
    
    logging.debug(f"Mapping technology abbreviations for file: {file_path}")
    # Add error handling for technology mapping
    try:
        unique_techs = df['technology'].unique()
        logging.debug(f"Found technology codes: {unique_techs}")
        df['tech_abbr'] = df['technology'].map(get_tech_abbr)
        unknown_techs = df[df['tech_abbr'] == "Unknown"]['technology'].unique()
        if len(unknown_techs) > 0:
            logging.warning(f"Unknown technology codes found in {file_path}: {unknown_techs}")
    except Exception as e:
        logging.error(f"Error mapping technology codes to abbreviations: {str(e)}")
        df['tech_abbr'] = "Unknown"
    
    logging.debug(f"Mapping holding company names for file: {file_path}")
    try:
        df['holding_company'] = df['provider_id'].map(lambda x: get_holder_name(x, holder_mapping))
    except Exception as e:
        logging.error(f"Error mapping provider_id to holding company: {str(e)}")
        df['holding_company'] = "Unknown"
    
    return df

def merge_data(tabblock_df, bdc_file_paths, holder_mapping):
    # Set CRS to EPSG:4269 (NAD83) for tabblock_df
    logging.debug("Setting CRS to EPSG:4269 for tabblock_df")
    tabblock_df = tabblock_df.to_crs(epsg=4269)

    # Convert tabblock_df to GeoJSON format
    logging.debug("Converting tabblock_df to GeoJSON format")
    tabblock_geojson = json.loads(tabblock_df.to_json())

    # Set up tqdm for pandas
    tqdm.pandas()

    # Process BDC files
    bdc_df_list = []
    for file_path in bdc_file_paths:
        bdc_df = process_bdc_file(file_path, holder_mapping)
        bdc_df_list.append(bdc_df)
    bdc_df = pd.concat(bdc_df_list, ignore_index=True)

    # Add tech_abbr to BDC data
    bdc_df['tech_abbr'] = bdc_df['technology'].progress_map(get_tech_abbr)
    logging.info("Finished adding tech_abbr")    

    # Add holding_company to BDC data
    bdc_df['holding_company'] = bdc_df['provider_id'].progress_map(lambda x: get_holder_name(x, holder_mapping))
    logging.info("Finished adding holding_company")    

    # Group BDC data with progress bar
    grouped_bdc = bdc_df.groupby(['block_geoid', 'technology', 'tech_abbr', 'provider_id', 'holding_company', 'brand_name', 'location_id']).agg({
        'max_advertised_download_speed': 'max',
        'max_advertised_upload_speed': 'max',
        'low_latency': 'max',
        'business_residential_code': 'max'
    }).reset_index()
    logging.info("Finished grouping BDC files")

    # Create a dictionary to map block_geoid to bdc_locations
    bdc_locations_dict = {}
    for block_geoid, group in tqdm(grouped_bdc.groupby('block_geoid'), desc="Creating bdc_locations_dict"):
        bdc_locations_dict[block_geoid] = group.to_dict(orient='records')
    logging.info("Finished grouping bdc_locations_dict")

    # Sort tabblock GeoJSON features by GEOID20
    logging.debug("Sorting tabblock GeoJSON features by GEOID20")
    tabblock_geojson['features'].sort(key=lambda x: x['properties']['GEOID20'])

    # Sort bdc_locations_dict by block_geoid
    logging.debug("Sorting bdc_locations_dict by block_geoid")
    sorted_bdc_locations_dict = dict(sorted(bdc_locations_dict.items()))

    # Merge BDC data with tabblock GeoJSON
    logging.debug("Merging BDC data with tabblock GeoJSON")
    for feature in tqdm(tabblock_geojson['features'], desc="Merging records"):
        geoid20 = feature['properties']['GEOID20']
        feature['properties']['bdc_locations'] = sorted_bdc_locations_dict.get(geoid20, [])
    logging.info("Finished merging records")

    return tabblock_geojson