import sys
import argparse
import logging
import geopandas as gpd
import pandas as pd
import json
import os
from constants import STATES_AND_TERRITORIES, TECH_ABBR_MAPPING
from prepdf import create_technology_code_df, create_provider_df
from aggstatedata import aggregate_state_data

def parse_args():
    parser = argparse.ArgumentParser(
        description='Process FCC Broadband Data Collection (BDC) files and merge with Census blocks.',
        epilog='''
Usage Examples:
  python main.py --base-dir /path/to/data --format json --merge-format gpkg
  python main.py --base-dir . --state CA --format csv

  # Exspected directory structure
    /path/to/data/
    ├── USA_Census
    │   ├── 01_AL_Alabama
    │   │   ├── tl_01_tabblock20.cpg -> ./tl_2024_01_tabblock20/tl_2024_01_tabblock20.cpg
    │   │   ├── tl_01_tabblock20.dbf -> ./tl_2024_01_tabblock20/tl_2024_01_tabblock20.dbf
    │   │   ├── tl_01_tabblock20.prj -> ./tl_2024_01_tabblock20/tl_2024_01_tabblock20.prj
    │   │   ├── tl_01_tabblock20.shp -> ./tl_2024_01_tabblock20/tl_2024_01_tabblock20.shp
    │   │   ├── tl_01_tabblock20.shp.ea.iso.xml -> ./tl_2024_01_tabblock20/tl_2024_01_tabblock20.shp.ea.iso.xml
    │   │   ├── tl_01_tabblock20.shp.iso.xml -> ./tl_2024_01_tabblock20/tl_2024_01_tabblock20.shp.iso.xml
    │   │   ├── tl_01_tabblock20.shx -> ./tl_2024_01_tabblock20/tl_2024_01_tabblock20.shx
    │   │   └── tl_2024_01_tabblock20
    │   │       ├── tl_2024_01_tabblock20.cpg
    │   │       ├── tl_2024_01_tabblock20.dbf
    │   │       ├── tl_2024_01_tabblock20.prj
    │   │       ├── tl_2024_01_tabblock20.shp
    │   │       ├── tl_2024_01_tabblock20.shp.ea.iso.xml
    │   │       ├── tl_2024_01_tabblock20.shp.iso.xml
    │   │       └── tl_2024_01_tabblock20.shx
    ...

    ├── USA_FCC-bdc
    │   ├── 01_AL_Alabama
    │   │   ├── bdc
    │   │   │   ├── bdc_01_Cable_fixed_broadband_J24_24dec2024.zip
    │   │   │   ├── bdc_01_Copper_fixed_broadband_J24_24dec2024.zip
    │   │   │   ├── bdc_01_FibertothePremises_fixed_broadband_J24_24dec2024.zip
    │   │   │   ├── bdc_01_GSOSatellite_fixed_broadband_J24_24dec2024.zip
    │   │   │   ├── bdc_01_LBRFixedWireless_fixed_broadband_J24_24dec2024.zip
    │   │   │   ├── bdc_01_LicensedFixedWireless_fixed_broadband_J24_24dec2024.zip
    │   │   │   ├── bdc_01_NGSOSatellite_fixed_broadband_J24_24dec2024.zip
    │   │   │   ├── bdc_01_Other_fixed_broadband_J24_24dec2024.zip
    │   │   │   ├── bdc_01_UnlicensedFixedWireless_fixed_broadband_J24_24dec2024.zip
    │   │   │   └── bdc_01_fixed_broadband_summary_by_geography_place_J24_24dec2024.zip
    │   │   ├── bdc_01_AL_BB.gpkg
    │   │   ├── bdc_01_AL_BB.gpkg-shm
    │   │   ├── bdc_01_AL_BB.gpkg-wal
    │   │   ├── bdc_01_AL_BB.json
    │   │   ├── bdc_01_Cable_fixed_broadband.zip -> ./bdc/bdc_01_Cable_fixed_broadband_J24_24dec2024.zip
    │   │   ├── bdc_01_Copper_fixed_broadband.zip -> ./bdc/bdc_01_Copper_fixed_broadband_J24_24dec2024.zip
    │   │   ├── bdc_01_FibertothePremises_fixed_broadband.zip -> ./bdc/bdc_01_FibertothePremises_fixed_broadband_J24_24dec2024.zip
    │   │   ├── bdc_01_GSOSatellite_fixed_broadband.zip -> ./bdc/bdc_01_GSOSatellite_fixed_broadband_J24_24dec2024.zip
    │   │   ├── bdc_01_LBRFixedWireless_fixed_broadband.zip -> ./bdc/bdc_01_LBRFixedWireless_fixed_broadband_J24_24dec2024.zip
    │   │   ├── bdc_01_LicensedFixedWireless_fixed_broadband.zip -> ./bdc/bdc_01_LicensedFixedWireless_fixed_broadband_J24_24dec2024.zip
    │   │   ├── bdc_01_NGSOSatellite_fixed_broadband.zip -> ./bdc/bdc_01_NGSOSatellite_fixed_broadband_J24_24dec2024.zip
    │   │   ├── bdc_01_Other_fixed_broadband.zip -> ./bdc/bdc_01_Other_fixed_broadband_J24_24dec2024.zip
    │   │   └── bdc_01_UnlicensedFixedWireless_fixed_broadband.zip -> ./bdc/bdc_01_UnlicensedFixedWireless_fixed_broadband_J24_24dec2024.zip
...
    ├── USA_FCC-bdc
    │   ├── resources
    │   │   ├── bdc-Fixed-and-Mobile-Technology-Codes.zip
    │   │   ├── bdc_us_provider_list_J24_24dec2024.zip
...
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Input/Output options
    io_group = parser.add_argument_group('Input/Output Options')
    io_group.add_argument(
        '--base-dir',
        default=os.getcwd(),
        help='Base directory containing USA_FCC-bdc and USA_Census folders, default is the current working directory'
    )

    io_group.add_argument(
        '--state',
        help='Process specific state by abbreviation (e.g., CA)'
    )

    # Format options
    format_group = parser.add_argument_group('Format Options')
    format_group.add_argument(
        '--format',
        choices=['json', 'csv'],
        default='json',
        help='Output format for BDC data (default: json)'
    )
    format_group.add_argument(
        '--merge-format',
        choices=['gpkg', 'geojson'],
        default='gpkg',
        help='Output format for Census block merges (default: gpkg)'
    )
    
    # Add version
    parser.add_argument('--version', 
                     action='version',
                     version='%(prog)s 1.0')

    return parser.parse_args()    

def main():
    args = parse_args()
    base_dir = args.base_dir

    # Validate base directory structure
    if not os.path.exists(base_dir):
        logging.error(f"Base directory not found: {base_dir}")
        logging.error("Please provide --base-dir argument pointing to SharedGeoData directory")
        sys.exit(1)
        
    # Check required subdirectories
    usa_fcc_dir = os.path.join(base_dir, 'USA_FCC-bdc')
    usa_census_dir = os.path.join(base_dir, 'USA_Census')
    resources_dir = os.path.join(usa_fcc_dir, 'resources')
    
    for directory in [usa_fcc_dir, usa_census_dir, resources_dir]:
        if not os.path.exists(directory):
            logging.error(f"Required directory not found: {directory}")
            logging.error(f"""Directory structure should be:
{base_dir}/
├── USA_FCC-bdc/
│   └── resources/
└── USA_Census/
            """)
            sys.exit(1)

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

     # Add debug logging for base_dir
    logging.debug(f"Base directory (raw): {base_dir}")
    logging.debug(f"Base directory (absolute): {os.path.abspath(base_dir)}")
    
    if not os.path.exists(base_dir):
        logging.error(f"Base directory not found: {base_dir}")
        sys.exit(1)
    
    logging.debug(f"Base directory contents: {os.listdir(base_dir)}")
       
    # Create DataFrames using base_dir
    technology_code_df = create_technology_code_df(base_dir)
    provider_df = create_provider_df(base_dir)

    # Filter states with explicit tuple unpacking and debug logging
    selected_states = STATES_AND_TERRITORIES
    if args.state:
        selected_states = [(fips, abbr, name) 
                          for fips, abbr, name in STATES_AND_TERRITORIES 
                          if abbr.upper() == args.state.upper()]
        if not selected_states:
            logging.error(f"State {args.state} not found")
            sys.exit(1)
        fips, abbr, name = selected_states[0]
        logging.debug(f"Selected state data: FIPS={fips}, Abbr={abbr}, Name={name}")

    try:
        aggregate_state_data(
            selected_states,
            technology_code_df,
            provider_df,
            output_format=args.format,
            base_dir=base_dir
        )
    except Exception as e:
        logging.error(f"Processing failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()