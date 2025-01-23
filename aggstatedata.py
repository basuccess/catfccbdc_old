import pandas as pd
import zipfile
import os
import re
import time
import psutil
import numpy as np
import json
import geopandas as gpd
from datetime import datetime
import logging
from constants import TECH_ABBR_MAPPING, STATES_AND_TERRITORIES

def log_memory_usage(context=""):
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024 / 1024
    logging.info(f"Memory usage {context}: {mem_mb:.1f} MB")

# Pre-compile regex pattern
BDC_FILE_PATTERN = re.compile(r'(bdc_\d{2}.*_fixed_broadband_)([A-Z]\d{2}_)(.*)\.(csv|zip)')

def read_state_file(file_path):
    dtype_map = {
        'frn': 'string',
        'provider_id': 'string',
        'brand_name': 'string',
        'location_id': 'string',
        'technology': 'int32',
        'max_advertised_download_speed': 'int32',
        'max_advertised_upload_speed': 'int32',
        'block_geoid': 'string',
        'h3_res8_id': 'string'
    }
    return pd.read_csv(file_path, dtype=dtype_map)

def create_tech_provider_maps(technology_code_df, provider_df):
    """Create mapping dictionaries for technology codes and provider IDs"""
    tech_map = TECH_ABBR_MAPPING
    logging.debug(f"Using technology mapping: {tech_map}")
    
    provider_map = dict(zip(
        provider_df['provider_id'].astype(int),
        provider_df['holding_company']
    ))
    return tech_map, provider_map

def write_output(output_df, fips_code, abbr, state_name, base_dir, format='json'):
    """Write output data to file in specified format"""
    state_dir = f"{fips_code}_{abbr}_{state_name.replace(' ', '_')}"
    output_path = os.path.join(base_dir, 'USA_FCC-bdc', state_dir, f"bdc_{fips_code}_{abbr}_BB.{format}")
    
    if format.lower() == 'json':
        json_data = {
            "type": "FeatureCollection",
            "features": []
        }
        for _, row in output_df.iterrows():
            feature = {
                "type": "Feature",
                "id": row['block_geoid'],
                "properties": {k: v for k, v in row.items() if k != 'block_geoid'}
            }
            json_data['features'].append(feature)
            
        with open(output_path, 'w') as f:
            json.dump(json_data, f, indent=2)
    else:
        output_df.to_csv(output_path, index=False)
        with zipfile.ZipFile(f"{output_path[:-4]}.zip", 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(output_path, os.path.basename(output_path))
        os.remove(output_path)

def aggregate_state_data(states_and_territories, technology_code_df, provider_df, output_format='json', base_dir=None):
    if base_dir is None:
        raise ValueError("base_dir parameter is required")
    
    total_start_time = time.time()
    total_states = len(states_and_territories)
    logging.info(f"Starting processing of {total_states} states")
    
    tech_map, provider_map = create_tech_provider_maps(technology_code_df, provider_df)
    
    for state_idx, state_tuple in enumerate(states_and_territories, 1):
        fips_code, abbr, state_name = state_tuple
        fips_code_str = f"{int(fips_code):02d}"
        state_start_time = time.time()
        
        try:
            state_dir = f"{fips_code_str}_{abbr}_{state_name.replace(' ', '_')}"
            bdc_dir = os.path.join(base_dir, 'USA_FCC-bdc', state_dir, 'bdc')
            
            logging.debug(f"Processing state: FIPS={fips_code_str}, Abbr={abbr}, Name={state_name}")
            logging.debug(f"BDC directory: {bdc_dir}")
            
            if not os.path.exists(bdc_dir):
                logging.warning(f"BDC directory not found: {bdc_dir}")
                continue
                
            valid_files = [f for f in os.listdir(bdc_dir) if BDC_FILE_PATTERN.match(f) and not f.startswith('._')]
            
            if not valid_files:
                logging.warning(f"No valid BDC files found in: {bdc_dir}")
                continue
                
            logging.info(f"Found {len(valid_files)} valid BDC files")
            
            state_data = []
            for file_idx, file_name in enumerate(valid_files, 1):
                file_path = os.path.join(bdc_dir, file_name)
                df = read_state_file(file_path)
                
                logging.debug(f"File {file_name} technology codes: {df['technology'].unique()}")
                
                df['technology_abbr'] = df['technology'].astype(int).map(tech_map)
                if df['technology_abbr'].isna().any():
                    missing_codes = df[df['technology_abbr'].isna()]['technology'].unique()
                    logging.error(f"Missing technology mappings for codes: {missing_codes}")
                    continue
                
                df['provider_name'] = df['provider_id'].astype(int).map(provider_map)
                state_data.append(df)
            
            if state_data:
                state_df = pd.concat(state_data, ignore_index=True)
                logging.debug(f"DataFrame shape: {state_df.shape}")
                logging.debug(f"DataFrame columns: {state_df.columns.tolist()}")
                logging.debug(f"Technology abbreviations: {state_df['technology_abbr'].unique()}")
                
                try:
                    provider_stats = (state_df.groupby(['block_geoid', 'technology_abbr', 'provider_name'])
                        .agg({
                            'location_id': 'nunique',
                            'max_advertised_download_speed': 'max',
                            'max_advertised_upload_speed': 'max'
                        })
                        .reset_index())
                except KeyError as e:
                    logging.error(f"Missing column in groupby: {e}")
                    raise
                
                chunk_size = 1000
                output_data = []
                total_blocks = provider_stats['block_geoid'].nunique()
                
                for chunk_idx, block_group in enumerate(np.array_split(provider_stats['block_geoid'].unique(), total_blocks // chunk_size + 1)):
                    if chunk_idx % 10 == 0:
                        logging.info(f"Processing block chunk {chunk_idx * chunk_size}/{total_blocks}")
                    
                    chunk_stats = provider_stats[provider_stats['block_geoid'].isin(block_group)]
                    
                    for block_geoid in block_group:
                        block_data = chunk_stats[chunk_stats['block_geoid'] == block_geoid]
                        row = {'block_geoid': block_geoid}
                        
                        unique_techs = block_data['technology_abbr'].unique()
                        for tech_abbr in unique_techs:
                            tech_data = block_data[block_data['technology_abbr'] == tech_abbr]
                    #        if not tech_data.empty:
                            row[f'{tech_abbr}_prov'] = tech_data['provider_name'].tolist()
                            row[f'{tech_abbr}_loc'] = tech_data['location_id'].tolist()
                            row[f'{tech_abbr}_tot'] = tech_data['location_id'].sum()
                            row[f'{tech_abbr}_down'] = tech_data['max_advertised_download_speed'].tolist()
                            row[f'{tech_abbr}_up'] = tech_data['max_advertised_upload_speed'].tolist()
                    #        else:
                    #            logging.debug(f"No data for tech_abbr: {tech_abbr} in block_geoid: {block_geoid}")
                    #            row[f'{tech_abbr}_prov'] = [""]
                    #            row[f'{tech_abbr}_loc'] = [0]
                    #            row[f'{tech_abbr}_tot'] = 0.0
                    #            row[f'{tech_abbr}_down'] = [0]
                    #            row[f'{tech_abbr}_up'] = [0]
                        
                        output_data.append(row)
                
                output_df = pd.DataFrame(output_data)
                
                # Add debug statement to print a sample of 10 rows of the output_df
                logging.debug(f"Sample output_df:\n{output_df.sample(10)}")
                
                write_output(output_df, fips_code_str, abbr, state_name, base_dir, format=output_format)
                
                state_time = time.time() - state_start_time
                logging.info(f"Completed {state_name} in {state_time:.1f} seconds")
                
        except Exception as e:
            logging.error(f"Error processing state {state_name}: {e}")
            continue

    total_time = time.time() - total_start_time
    logging.info(f"Completed all states in {total_time:.1f} seconds")
    log_memory_usage("final")