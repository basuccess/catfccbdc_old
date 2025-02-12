import os
import argparse
import logging
import sys  # Import sys module
from constant import STATES_AND_TERRITORIES
from prepdata import prepare_data, load_holder_mapping
from readin import read_data, check_required_files
from merge import merge_data
from writeout import write_geojson_and_convert_to_gpkg

def setup_logging(log_file, base_dir):
    if log_file is not None:
        log_file_path = os.path.join(base_dir, log_file)
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file_path, filemode='w')
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def expand_state_ranges(state_args):
    state_abbrs = [state[1] for state in STATES_AND_TERRITORIES]
    expanded_states = []
    for arg in state_args:
        if '..' in arg:
            start, end = arg.split('..')
            start_index = state_abbrs.index(start)
            end_index = state_abbrs.index(end)
            expanded_states.extend(state_abbrs[start_index:end_index + 1])
        else:
            expanded_states.append(arg)
    logging.debug(f'Expanded state ranges: {expanded_states}')
    return expanded_states

def parse_arguments():
    parser = argparse.ArgumentParser(description='Build broadband service geopackage files for US states and territories.')
    parser.add_argument('-d', '--base-dir', type=str, default=os.getcwd(), help='Base directory for data files')
    parser.add_argument('-s', '--state', type=str, nargs='*', default=[state[1] for state in STATES_AND_TERRITORIES], help='State abbreviation(s) to process')
    parser.add_argument('--log-file', type=str, nargs='?', const='catfccbdc_log.log', help='Log file path')
    parser.add_argument('-o', '--output-dir', type=str, help='Output directory for data files')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.0', help='Print version and exit')
    args = parser.parse_args()
    args.state = expand_state_ranges(args.state)
    logging.info(f'States to be processed: {args.state}')
    return args

def main():
    args = parse_arguments()
    setup_logging(args.log_file, args.base_dir)

    base_dir = args.base_dir
    output_dir = args.output_dir
    states_to_process = args.state

    logging.info(f'Starting processing for states: {states_to_process} in base directory: {base_dir}')
    
    holder_mapping = load_holder_mapping(base_dir)
    logging.debug(f'Loaded holder mapping: {holder_mapping}')

    for state in states_to_process:
        logging.info(f'Processing state: {state}')
        try:
            prepare_data(base_dir, state)
            logging.info(f'Finished preparing data for state: {state}')
            
            tabblock_data = read_data(base_dir, state)
            logging.info(f'Finished reading tabblock data for state: {state}')
            
            bdc_data = read_data(base_dir, state, bdc=True)
            logging.info(f'Finished reading BDC data for state: {state}')
            
            merged_data = merge_data(tabblock_data, bdc_data, holder_mapping)
            logging.info(f'Finished merging data for state: {state}')
            
            write_geojson_and_convert_to_gpkg(merged_data, base_dir, state, output_dir)
            logging.info(f'Finished writing GeoJSON and GeoPackage for state: {state}')
            
        except FileNotFoundError as e:
            logging.warning(f"Skipped state {state}: {e}")
        except Exception as e:
            logging.error(f"Error processing state {state}: {e}")

if __name__ == '__main__':
    main()

# Test function to simulate argument parsing
def test_expand_state_ranges():
    test_args = ['AL', 'CA', 'AL..CT', 'MO', 'NJ..ND']
    expanded_states = expand_state_ranges(test_args)
    print(f'Resulting list of states: {expanded_states}')

# Uncomment the following line to run the test function
# test_expand_state_ranges()