# merge.py

import pandas as pd
import geopandas as gpd
import json
import logging
from tqdm import tqdm
from constant import TECH_ABBR_MAPPING, BDC_US_PROVIDER_FILE_PATTERN, \
    BDC_TECH_CODES_FILE_PATTERN, BDC_FILE_PATTERN, TABBLOCK20_FILE_PATTERN
from functions import monitor_memory
from bdcfunctions import get_tech_abbr, get_holder_name
import psutil  # Import psutil for memory monitoring
import gc  # Import garbage collector
from dask import dataframe as dd
from dask.distributed import Client
from collections import defaultdict
from multiprocessing import cpu_count  # Import cpu_count

# Merge BDC data with tabblock data

def process_bdc_file(file_path, holder_mapping):
    df = gpd.read_file(file_path)
    monitor_memory()  # Monitor memory usage after reading BDC file

    # Ensure block_geoid is treated as a string
    df['block_geoid'] = df['block_geoid'].astype(str)

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
    monitor_memory()  # Monitor memory usage after mapping technology abbreviations
    
    logging.debug(f"Mapping holding company names for file: {file_path}")
    try:
        df['holding_company'] = df['provider_id'].map(lambda x: get_holder_name(x, holder_mapping))
    except Exception as e:
        logging.error(f"Error mapping provider_id to holding company: {str(e)}")
        df['holding_company'] = "Unknown"
    monitor_memory()  # Monitor memory usage after mapping holding company names
    
    return df

def process_chunk(chunk):
    grouped_chunk = chunk.groupby(['block_geoid', 'technology', 'tech_abbr', 'provider_id', 'holding_company', 'brand_name', 'location_id']).agg({
        'max_advertised_download_speed': 'max',
        'max_advertised_upload_speed': 'max',
        'low_latency': 'max',
        'business_residential_code': 'max'
    }).reset_index()
    return grouped_chunk

def merge_data(tabblock_df, bdc_file_paths, holder_mapping, chunk_size=5000):  # Increased chunk size
    # Set CRS to EPSG:4269 (NAD83) for tabblock_df
    logging.debug("Setting CRS to EPSG:4269 for tabblock_df")
    tabblock_df = tabblock_df.to_crs(epsg=4269)
    monitor_memory()  # Monitor memory usage after setting CRS

    # Convert tabblock_df to GeoJSON format
    logging.debug("Converting tabblock_df to GeoJSON format")
    tabblock_geojson = json.loads(tabblock_df.to_json())
    monitor_memory()  # Monitor memory usage after converting to GeoJSON

    # Sort tabblock GeoJSON features by GEOID20
    logging.debug("Sorting tabblock GeoJSON features by GEOID20")
    tabblock_geojson['features'].sort(key=lambda x: x['properties']['GEOID20'])
    monitor_memory()  # Monitor memory usage after sorting tabblock GeoJSON features

    # Set up tqdm for pandas
    tqdm.pandas()

    # Process BDC files in chunks
    bdc_df_list = []
    for file_path in bdc_file_paths:
        bdc_df = process_bdc_file(file_path, holder_mapping)
        bdc_df_list.append(bdc_df)
    bdc_df = pd.concat(bdc_df_list, ignore_index=True)
    monitor_memory()  # Monitor memory usage after processing BDC files

    # Ensure block_geoid is treated as a string
    bdc_df['block_geoid'] = bdc_df['block_geoid'].astype(str)

    # Convert to Dask DataFrame
    ddf = dd.from_pandas(bdc_df, npartitions=cpu_count())

    # Process BDC data in chunks using Dask
    bdc_locations_dict = defaultdict(list)
    grouped_ddf = ddf.groupby('block_geoid').apply(process_chunk, meta=bdc_df).compute()

    for block_geoid, group in grouped_ddf.groupby('block_geoid'):
        bdc_locations_dict[block_geoid].extend(group.to_dict(orient='records'))
        monitor_memory()  # Monitor memory usage after processing each chunk

    logging.info("Finished processing BDC data in chunks")
    monitor_memory()  # Monitor memory usage after processing all chunks

    # Merge BDC data with tabblock GeoJSON
    logging.debug("Merging BDC data with tabblock GeoJSON")
    for feature in tqdm(tabblock_geojson['features'], desc="Merging records"):
        geoid20 = feature['properties']['GEOID20']
        feature['properties']['bdc_locations'] = bdc_locations_dict.get(str(geoid20), [])
    logging.info("Finished merging records")
    monitor_memory()  # Monitor memory usage after merging records

    return tabblock_geojson

# Write out the merged data to GeoJSON and GeoPackage files

def transform_bdc_locations(bdc_locations, housing20):
    tech_map = {abbr: None for abbr, _ in TECH_ABBR_MAPPING.values()}
    location_ids = set()
    location_max_speeds = {}

    for location in bdc_locations:
        tech_code = int(location['technology'])  # Ensure tech_code is an integer
        tech_abbr = TECH_ABBR_MAPPING.get(tech_code, ("Unknown", False))[0]
        holding_company = location['holding_company']
        location_id = location['location_id']
        max_download_speed = location['max_advertised_download_speed']
        business_residential_code = location['business_residential_code']
        
        # Ensure max_download_speed is an integer
        try:
            max_download_speed = int(max_download_speed)
        except ValueError:
            logging.warning(f"Invalid max_download_speed value: {max_download_speed} for location_id: {location_id}")
            continue
        
        location_ids.add(location_id)
        
        if location_id not in location_max_speeds:
            location_max_speeds[location_id] = (max_download_speed, business_residential_code, tech_code)
        else:
            location_max_speeds[location_id] = (max(location_max_speeds[location_id][0], max_download_speed), business_residential_code, tech_code)

        # Initialize tech_map[tech_abbr] if it is None
        if tech_map[tech_abbr] is None:
            tech_map[tech_abbr] = {
                'holding_company': [],
                'locations': [],
                'provider_id': [],
                'brand_name': [],
                'technology': location['technology'],
                'technology_description': tech_abbr,
                'max_advertised_download_speed': [],
                'max_advertised_upload_speed': [],
                'low_latency': [],
                'business_residential_code': []
            }
        if holding_company not in tech_map[tech_abbr]['holding_company']:
            tech_map[tech_abbr]['holding_company'].append(holding_company)
            tech_map[tech_abbr]['locations'].append(0)
            tech_map[tech_abbr]['provider_id'].append(location['provider_id'])
            tech_map[tech_abbr]['brand_name'].append(location['brand_name'])
            tech_map[tech_abbr]['max_advertised_download_speed'].append(max_download_speed)
            tech_map[tech_abbr]['max_advertised_upload_speed'].append(location['max_advertised_upload_speed'])
            tech_map[tech_abbr]['low_latency'].append(location['low_latency'])
            tech_map[tech_abbr]['business_residential_code'].append(business_residential_code)
        index = tech_map[tech_abbr]['holding_company'].index(holding_company)
        tech_map[tech_abbr]['locations'][index] += 1

    # Filter location_max_speeds to only include relevant technologies for calculations
    filtered_max_speeds = {k: v for k, v in location_max_speeds.items() if TECH_ABBR_MAPPING[v[2]][1]}

    # Separate speeds by business_residential_code
    business_speeds = {k: v[0] for k, v in filtered_max_speeds.items() if v[1] == 'B'}
    residential_and_business_speeds = {k: v[0] for k, v in filtered_max_speeds.items() if v[1] == 'X'}
    residential_speeds = {k: v[0] for k, v in filtered_max_speeds.items() if v[1] == 'R'}

    # Convert speeds to integer for comparison
    business_speeds = {k: int(v) for k, v in business_speeds.items() if v is not None}
    residential_and_business_speeds = {k: int(v) for k, v in residential_and_business_speeds.items() if v is not None}
    residential_speeds = {k: int(v) for k, v in residential_speeds.items() if v is not None}

    # Calculate counts for residential locations
    residential_unserved_count = sum(1 for speed in residential_speeds.values() if speed < 25)
    residential_underserved_count = sum(1 for speed in residential_speeds.values() if 25 <= speed < 100)
    residential_served_count = sum(1 for speed in residential_speeds.values() if speed >= 100)

    # Calculate counts for residential_and_business locations
    residential_and_business_unserved_count = sum(1 for speed in residential_and_business_speeds.values() if speed < 25)
    residential_and_business_underserved_count = sum(1 for speed in residential_and_business_speeds.values() if 25 <= speed < 100)
    residential_and_business_served_count = sum(1 for speed in residential_and_business_speeds.values() if speed >= 100)

    # Calculate counts for business locations
    business_unserved_count = sum(1 for speed in business_speeds.values() if speed < 25)
    business_underserved_count = sum(1 for speed in business_speeds.values() if 25 <= speed < 100)
    business_served_count = sum(1 for speed in business_speeds.values() if speed >= 100)

    # Adjust residential_unserved_count based on HOUSING20
    if residential_unserved_count + residential_underserved_count + residential_served_count < housing20:
        residential_unserved_count = housing20 - residential_underserved_count - residential_served_count
    
    return tech_map, len(location_ids), residential_unserved_count, residential_underserved_count, residential_served_count, \
        residential_and_business_unserved_count, residential_and_business_underserved_count, residential_and_business_served_count, \
        business_unserved_count, business_underserved_count, business_served_count

def write_geojson_and_convert_to_gpkg(merged_data, base_dir, state_abbr, output_dir=None):
    # Set CRS to EPSG:4269 (NAD83)
    crs = "EPSG:4269"

    fips, abbr, name = get_state_info(state_abbr)
    if output_dir is None:
        state_dir = f"{fips}_{abbr}_{name}"
        output_dir = os.path.join(base_dir, 'USA_FCC-bdc', state_dir)
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    geojson_file = os.path.join(output_dir, f"{fips}_{abbr}_BB.geojson")
    gpkg_file = os.path.join(output_dir, f"{fips}_{abbr}_BB.gpkg")
    
    # Transform the bdc_locations field and add summary fields
    for feature in merged_data['features']:
        feature['id'] = feature['properties']['GEOID20']  # Set GEOID20 as the feature ID
        housing20 = feature['properties'].get('HOUSING20', 0)  # Get HOUSING20 value
        if 'bdc_locations' in feature['properties']:
            tech_map, total_locations, residential_unserved_count, residential_underserved_count, residential_served_count,\
                residential_and_business_unserved_count, residential_and_business_underserved_count, residential_and_business_served_count,\
                business_unserved_count, business_underserved_count, business_served_count = transform_bdc_locations(feature['properties']['bdc_locations'], housing20)
            feature['properties'].update(tech_map)
            feature['properties']['Total_Locations'] = total_locations
            feature['properties']['Residential_Unserved'] = residential_unserved_count
            feature['properties']['Residential_Underserved'] = residential_underserved_count
            feature['properties']['Residential_Served'] = residential_served_count
            feature['properties']['Residential_and_Business_Unserved'] = residential_and_business_unserved_count
            feature['properties']['Residential_and_Business_Underserved'] = residential_and_business_underserved_count
            feature['properties']['Residential_and_Business_Served'] = residential_and_business_served_count
            feature['properties']['Business_Unserved'] = business_unserved_count
            feature['properties']['Business_Underserved'] = business_underserved_count
            feature['properties']['Business_Served'] = business_served_count
            del feature['properties']['bdc_locations']
        else:
            # Ensure all technology fields are present even if there are no matched entries
            for tech_abbr, _ in TECH_ABBR_MAPPING.values():
                feature['properties'][tech_abbr] = None
            feature['properties']['Total_Locations'] = 0
            feature['properties']['Residential_Unserved'] = 0
            feature['properties']['Residential_Underserved'] = 0
            feature['properties']['Residential_Served'] = 0
            feature['properties']['Residential_and_Business_Unserved'] = 0
            feature['properties']['Residential_and_Business_Underserved'] = 0
            feature['properties']['Residential_and_Business_Served'] = 0
            feature['properties']['Business_Unserved'] = 0
            feature['properties']['Business_Underserved'] = 0
            feature['properties']['Business_Served'] = 0
    
    # Convert the merged data to a GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(merged_data['features'], crs=crs)

    # Validate geometries
    if not gdf.is_valid.all():
        logging.warning("Some geometries are invalid. Attempting to fix invalid geometries.")
        gdf = gdf.buffer(0)
        if not gdf.is_valid.all():
            logging.error("Failed to fix invalid geometries.")
            return

    # Write the GeoJSON file with progress indication and indentation
    with open(geojson_file, 'w') as f:
        json.dump(merged_data, f, indent=2)
    
    logging.info(f"GeoJSON file written to: {geojson_file}")
    
    # Delete the existing GeoPackage file if it exists
    if os.path.exists(gpkg_file):
        os.remove(gpkg_file)
    
    # Write the GeoDataFrame to a new GeoPackage file
    gdf.to_file(gpkg_file, driver="GPKG", layer=f"{fips}_{abbr}_BB")
    
    logging.info(f"GeoPackage file written to: {gpkg_file}")