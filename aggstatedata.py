import pandas as pd
import zipfile
import os
import re
import time
import psutil
import numpy as np
from datetime import datetime
import logging

# Pre-compile regex pattern
BDC_FILE_PATTERN = re.compile(r'(bdc_\d{2}.*_fixed_broadband_)([A-Z]\d{2}_)(.*)\.(csv|zip)')

def read_state_file(file_path):
    # Read state data file with optimized dtypes
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
    # Create mapping dictionaries for faster lookups
    tech_map = dict(zip(
        technology_code_df['Code'].astype(int),
        technology_code_df['Abbr'].str.strip()
    ))
    provider_map = dict(zip(
        provider_df['provider_id'].astype(int),
        provider_df['holding_company']
    ))
    return tech_map, provider_map

def process_block_data(group, tech_abbr):
    # Process data for a single block_geoid and technology
    tech_group = group[group['technology_abbr'] == tech_abbr]
    providers = tech_group['provider_name'].unique().tolist()
    
    location_counts = []
    max_downloads = []
    max_uploads = []
    
    for provider in providers:
        provider_group = tech_group[tech_group['provider_name'] == provider]
        location_counts.append(provider_group['location_id'].nunique())
        max_downloads.append(provider_group['max_advertised_download_speed'].max())
        max_uploads.append(provider_group['max_advertised_upload_speed'].max())
    
    return {
        'providers': providers,
        'location_counts': location_counts,
        'total_locations': sum(location_counts) if location_counts else 0,
        'max_downloads': max_downloads,
        'max_uploads': max_uploads
    }

def log_memory_usage(context=""):
    # Log current memory usage
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024 / 1024
    logging.info(f"Memory usage {context}: {mem_mb:.1f} MB")

def aggregate_state_data(states_and_territories, technology_code_df, provider_df):
    # Main function to aggregate state broadband data files
    total_start_time = time.time()
    total_states = len(states_and_territories)
    logging.info(f"Starting processing of {total_states} states")
    
    tech_map, provider_map = create_tech_provider_maps(technology_code_df, provider_df)
    
    for state_idx, (fips_code, abbr, state_name) in enumerate(states_and_territories, 1):
        state_start_time = time.time()
        logging.info(f"Processing state {state_name} ({state_idx}/{total_states})")
        
        try:
            fips_code_str = f"{int(fips_code):02d}"
            state_dir = f"{fips_code_str}_{abbr}_{state_name}"
            bdc_dir = os.path.join(state_dir, 'bdc')
            
            if not os.path.exists(bdc_dir):
                logging.warning(f"BDC directory not found: {bdc_dir}")
                continue

            # Process state files
            state_data = []
            valid_files = [f for f in os.listdir(bdc_dir) if BDC_FILE_PATTERN.match(f) and not f.startswith('._')]
            total_files = len(valid_files)
            
            file_start_time = time.time()
            for file_idx, file_name in enumerate(valid_files, 1):
                logging.info(f"Processing file {file_idx}/{total_files} for {state_name}: {file_name}")
                file_path = os.path.join(bdc_dir, file_name)
                
                try:
                    df = read_state_file(file_path)
                    df['technology_abbr'] = df['technology'].astype(int).map(tech_map)
                    df['provider_name'] = df['provider_id'].astype(int).map(provider_map)
                    state_data.append(df)
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")
                    continue

            if state_data:
                logging.info(f"Files processed in {time.time() - file_start_time:.1f} seconds")
                log_memory_usage("after file processing")

                # Process block data with optimizations
                block_start_time = time.time()
                state_df = pd.concat(state_data, ignore_index=True)
                state_data = None  # Free memory
                
                # Pre-compute all provider stats at once
                logging.info("Pre-computing provider statistics...")
                provider_stats = (state_df.groupby(['block_geoid', 'technology_abbr', 'provider_name'])
                    .agg({
                        'location_id': 'nunique',
                        'max_advertised_download_speed': 'max',
                        'max_advertised_upload_speed': 'max'
                    })
                    .reset_index())
                
                # Free original DataFrame
                state_df = None
                
                # Process in chunks
                chunk_size = 1000
                output_data = []
                total_blocks = provider_stats['block_geoid'].nunique()
                df_start_time = time.time()
                
                for chunk_idx, block_group in enumerate(np.array_split(provider_stats['block_geoid'].unique(), total_blocks // chunk_size + 1)):
                    if chunk_idx % 10 == 0:
                        current_block = min(chunk_idx * chunk_size, total_blocks)
                        logging.info(f"Processing block chunk {current_block}/{total_blocks}")
                    
                    # Get stats for current blocks
                    chunk_stats = provider_stats[provider_stats['block_geoid'].isin(block_group)]
                    
                    # Process each block
                    for block_geoid in block_group:
                        row = {'block_geoid': block_geoid}
                        block_data = chunk_stats[chunk_stats['block_geoid'] == block_geoid]
                        
                        for tech_abbr in technology_code_df['Abbr'].str.strip():
                            tech_data = block_data[block_data['technology_abbr'] == tech_abbr]
                            
                            if not tech_data.empty:
                                row[f'{tech_abbr}'] = tech_data['provider_name'].tolist()
                                row[f'{tech_abbr}L'] = tech_data['location_id'].tolist()
                                row[f'{tech_abbr}T'] = tech_data['location_id'].sum()
                                row[f'{tech_abbr}D'] = tech_data['max_advertised_download_speed'].astype(int).tolist()
                                row[f'{tech_abbr}U'] = tech_data['max_advertised_upload_speed'].astype(int).tolist()
                            else:
                                row[f'{tech_abbr}'] = []
                                row[f'{tech_abbr}L'] = []
                                row[f'{tech_abbr}T'] = 0
                                row[f'{tech_abbr}D'] = []
                                row[f'{tech_abbr}U'] = []
                        
                        output_data.append(row)
                    
                    # Create DataFrame periodically to save memory
                    if len(output_data) >= chunk_size * 2:
                        temp_df = pd.DataFrame(output_data)
                        output_df = pd.concat([output_df, temp_df]) if 'output_df' in locals() else temp_df
                        output_data = []
                
                # Process remaining data
                if output_data:
                    temp_df = pd.DataFrame(output_data)
                    output_df = pd.concat([output_df, temp_df]) if 'output_df' in locals() else temp_df

                # Ensure all columns exist
                for tech_abbr in technology_code_df['Abbr'].str.strip():
                    if f'{tech_abbr}' not in output_df.columns:
                        output_df[f'{tech_abbr}'] = output_df.apply(lambda x: [], axis=1)
                        output_df[f'{tech_abbr}L'] = output_df.apply(lambda x: [], axis=1)
                        output_df[f'{tech_abbr}T'] = 0
                        output_df[f'{tech_abbr}D'] = output_df.apply(lambda x: [], axis=1)
                        output_df[f'{tech_abbr}U'] = output_df.apply(lambda x: [], axis=1)
                    output_df[f'{tech_abbr}C'] = output_df[f'{tech_abbr}'].apply(len)

                # Create ordered column list
                columns = ['block_geoid']
                for tech_abbr in technology_code_df['Abbr'].str.strip():
                    columns.extend([
                        f'{tech_abbr}',      # provider names list
                        f'{tech_abbr}C',     # provider count
                        f'{tech_abbr}L',     # location counts list
                        f'{tech_abbr}T',     # total locations
                        f'{tech_abbr}D',     # max download speeds
                        f'{tech_abbr}U'      # max upload speeds
                    ])
                
                output_df = output_df[columns]
                logging.info(f"DataFrame creation completed in {time.time() - df_start_time:.1f} seconds")
                
                # Write and compress output
                output_path = os.path.join(state_dir, f"bdc_{fips_code_str}_{abbr}_BB.csv")
                output_df.to_csv(output_path, index=False)
                
                with zipfile.ZipFile(f"{output_path[:-4]}.zip", 'w', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(output_path, os.path.basename(output_path))
                os.remove(output_path)
                
                # Cleanup temporary files
                for file_name in os.listdir(bdc_dir):
                    if file_name.endswith('.csv') and not file_name.startswith('._'):
                        try:
                            os.remove(os.path.join(bdc_dir, file_name))
                        except Exception as e:
                            logging.error(f"Error removing file {file_name}: {e}")

                state_time = time.time() - state_start_time
                logging.info(f"Completed {state_name} in {state_time:.1f} seconds")
                log_memory_usage(f"after completing {state_name}")

        except Exception as e:
            logging.error(f"Error processing state {state_name}: {e}")
            continue

    total_time = time.time() - total_start_time
    logging.info(f"Completed all states in {total_time:.1f} seconds")
    log_memory_usage("final")