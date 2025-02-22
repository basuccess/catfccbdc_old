# main.py

import os
import logging
import sys
import traceback
import gc  # Import garbage collector
import psutil  # Import psutil for memory monitoring
from functions import setup_logging, parse_arguments, expand_state_ranges, monitor_memory
from bdcfunctions import read_data
from prep import get_state_info, load_holder_mapping, check_required_files, prepare_data 
from merge import merge_data, write_geojson_and_convert_to_gpkg

def main():
    # Parse arguments first
    args = parse_arguments()
    
    # Set up logging before any logging calls
    setup_logging(args.log_file, args.base_dir, args.log_level, args.log_parts)

    # Check base_dir and required files exists
    base_dir = args.base_dir
    check_required_files(base_dir)

    # Now expand state ranges after logging is configured
    try:
        states_to_process = expand_state_ranges(args.state)
        logging.info(f'States to be processed: {states_to_process}')
    except ValueError as e:
        logging.error("Error processing state ranges")
        return

    output_dir = args.output_dir

    logging.info(f'Starting processing for states in base directory: {base_dir}')
    
    try:
        holder_mapping = load_holder_mapping(base_dir)
        logging.debug(f'Loaded holder mapping: {holder_mapping}')
    except FileNotFoundError as e:
        logging.error(f"Resources directory not found: {e}")
        sys.exit(1)
    
    monitor_memory()  # Monitor memory usage after loading holder mapping

    for state in states_to_process:
        logging.info(f'Processing state: {state}')
        try:
            prepare_data(base_dir, state)
            logging.info(f'Finished preparing data for state: {state}')
            monitor_memory()  # Monitor memory usage after preparing data
            
            tabblock_data = read_data(base_dir, state)
            logging.info(f'Finished reading tabblock data for state: {state}')
            monitor_memory()  # Monitor memory usage after reading tabblock data
            
            bdc_file_paths = read_data(base_dir, state, bdc=True)
            logging.info(f'Found BDC files for state: {state}')
            monitor_memory()  # Monitor memory usage after reading BDC file paths
            
            merged_data = merge_data(tabblock_data, bdc_file_paths, holder_mapping)
            logging.info(f'Finished merging data for state: {state}')
            monitor_memory()  # Monitor memory usage after merging data
            
            # Determine the output directory for the current state
            fips, abbr, name = get_state_info(state)
            if output_dir is None:
                state_output_dir = os.path.join(base_dir, 'USA_FCC-bdc', f"{fips}_{abbr}_{name}")
            else:
                state_output_dir = output_dir

            # Ensure the output directory exists
            os.makedirs(state_output_dir, exist_ok=True)

            write_geojson_and_convert_to_gpkg(merged_data, base_dir, state, state_output_dir)
            logging.info(f'Finished writing GeoJSON and GeoPackage for state: {state}')
            monitor_memory()  # Monitor memory usage after writing output
            
        except FileNotFoundError as e:
            logging.warning(f"Skipped state {state}: {e}")
        except Exception as e:
            logging.error(f"Error processing state {state}: {e}")
            logging.error(traceback.format_exc())
        finally:
            # Clear variables and force garbage collection
            if 'tabblock_data' in locals():
                del tabblock_data
            if 'bdc_file_paths' in locals():
                del bdc_file_paths
            if 'merged_data' in locals():
                del merged_data
            gc.collect()
            logging.info(f'Cleared memory for state: {state}')
            monitor_memory()  # Monitor memory usage after each state        

if __name__ == '__main__':
    main()