# main.py

import os
import argparse
import logging
import sys
import traceback
from constant import STATES_AND_TERRITORIES, TECH_ABBR_MAPPING
from prepdata import prepare_data, load_holder_mapping
from readin import read_data, check_required_files, get_state_info
from merge import merge_data
from writeout import write_geojson_and_convert_to_gpkg

def setup_logging(log_file, base_dir):
    log_format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    log_level = logging.INFO

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file is not None:
        log_file_path = os.path.join(base_dir, log_file)
        handlers.append(logging.FileHandler(log_file_path, mode='w'))
        log_level = logging.DEBUG

    # Remove any existing handlers to avoid duplicates
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    logging.basicConfig(level=log_level, format=log_format, handlers=handlers, force=True)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Build broadband service geopackage files for US states and territories.')
    parser.add_argument('-d', '--base-dir', type=str, default=os.getcwd(), help='Base directory for data files')
    parser.add_argument('-s', '--state', type=str, nargs='*', default=[state[1] for state in STATES_AND_TERRITORIES], help='State abbreviation(s) to process')
    parser.add_argument('--log-file', type=str, nargs='?', const='catfccbdc_log.log', help='Log file path')
    parser.add_argument('-o', '--output-dir', type=str, help='Output directory for data files')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 2.0', help='Print version and exit')
    return parser.parse_args()

def expand_state_ranges(states):
    state_abbrs = [state[1] for state in STATES_AND_TERRITORIES]
    expanded_states = []
    
    for state in states:
        if '..' in state:
            start, end = state.split('..')
            if start not in state_abbrs or end not in state_abbrs:
                raise ValueError(f"Invalid state range: {state}")
            start_index = state_abbrs.index(start)
            end_index = state_abbrs.index(end)
            if start_index > end_index:
                raise ValueError(f"Invalid state range: {state}")
            expanded_states.extend(state_abbrs[start_index:end_index + 1])
        else:
            if state not in state_abbrs:
                raise ValueError(f"Invalid state abbreviation: {state}")
            expanded_states.append(state)
    
    return expanded_states

def main():
    # Parse arguments first
    args = parse_arguments()
    
    # Set up logging before any logging calls
    setup_logging(args.log_file, args.base_dir)
    
    # Now expand state ranges after logging is configured
    try:
        states_to_process = expand_state_ranges(args.state)
        logging.info(f'States to be processed: {states_to_process}')
    except ValueError as e:
        logging.error("Error processing state ranges")
        return

    base_dir = args.base_dir
    output_dir = args.output_dir

    logging.info(f'Starting processing for states in base directory: {base_dir}')
    
    holder_mapping = load_holder_mapping(base_dir)
    logging.debug(f'Loaded holder mapping: {holder_mapping}')

    for state in states_to_process:
        logging.info(f'Processing state: {state}')
        try:
            prepare_data(base_dir, state)
            logging.info(f'Finished preparing data for state: {state}')
            
            tabblock_data = read_data(base_dir, state)
            logging.info(f'Finished reading tabblock data for state: {state}')
            
            bdc_file_paths = read_data(base_dir, state, bdc=True)
            logging.info(f'Found BDC files for state: {state}')
            
            merged_data = merge_data(tabblock_data, bdc_file_paths, holder_mapping)
            logging.info(f'Finished merging data for state: {state}')
            
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
            
        except FileNotFoundError as e:
            logging.warning(f"Skipped state {state}: {e}")
        except Exception as e:
            logging.error(f"Error processing state {state}: {e}")
            logging.error(traceback.format_exc())

if __name__ == '__main__':
    main()