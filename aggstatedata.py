import pandas as pd
import zipfile
import os
import re
from datetime import datetime
import logging

def aggregate_state_data(states_and_territories, technology_code_df, provider_df):
    logging.debug("Aggregating state data")
    required_columns = [
        'frn', 'provider_id', 'brand_name', 'location_id', 'technology',
        'max_advertised_download_speed', 'max_advertised_upload_speed', 'low_latency',
        'business_residential_code', 'state_usps', 'block_geoid', 'h3_res8_id'
    ]

    # Log the columns in technology_code_df and provider_df to verify the column names
    logging.debug(f"Technology Code DataFrame columns: {technology_code_df.columns.tolist()}")
    logging.debug(f"Provider DataFrame columns: {provider_df.columns.tolist()}")

    # Create lookup dictionaries once
    tech_code_to_abbr = dict(zip(technology_code_df['Code'], technology_code_df['Abbr'].str.strip()))
    provider_id_to_name = dict(zip(provider_df['provider_id'], provider_df['holding_company']))
 
    # Start step-by-step processing for each state or territory
    for fips_code, abbr, state_name in states_and_territories:
        fips_code_str = f"{int(fips_code):02d}"  # Ensure fips_code is 2 digits with leading zeros
        state_dir = f"{fips_code_str}_{abbr}_{state_name}"
        bdc_dir = os.path.join(state_dir, 'bdc')
        if not os.path.exists(bdc_dir):
            logging.warning(f"BDC directory not found: {bdc_dir}")
            continue

        logging.debug(f"Processing BDC directory: {bdc_dir}")
        state_data = []
        files_to_process = {}

        # Step 1: Find all potential files that match the pattern
        logging.debug("Step 1: Finding all potential files that match the pattern")
        pattern = re.compile(r'(bdc_\d{2}.*_fixed_broadband_)([A-Z]\d{2}_)(.*)\.(csv|zip)')
        for file_name in os.listdir(bdc_dir):
            match = pattern.match(file_name)
            if match:
                base_name = match.group(1) + match.group(2)
                date_str = match.group(3)
                file_type = match.group(4)
                if base_name not in files_to_process:
                    files_to_process[base_name] = []
                files_to_process[base_name].append((file_name, date_str, file_type))

        # Step 2: Remove .csv files if both .csv and .zip exist
        logging.debug("Step 2: Removing .csv files if both .csv and .zip exist")
        for base_name, file_list in files_to_process.items():
            csv_files = [f for f in file_list if f[2] == 'csv']
            zip_files = [f for f in file_list if f[2] == 'zip']
            if csv_files and zip_files:
                files_to_process[base_name] = zip_files

        # Step 3: Unzip the matched .zip files
        logging.debug("Step 3: Unzipping the matched .zip files")
        for base_name, file_list in files_to_process.items():
            for file_name, date_str, file_type in file_list:
                file_path = os.path.join(bdc_dir, file_name)
                if file_type == 'zip':
                    try:
                        with zipfile.ZipFile(file_path, 'r') as zip_ref:
                            zip_ref.extractall(bdc_dir)
                    except zipfile.BadZipFile:
                        logging.warning(f"Bad ZIP file: {file_path}")
                        continue

        # Step 4: Check for required columns in CSV files
        logging.debug("Step 4: Checking for required columns in CSV files")
        valid_files = []
        for file_name in os.listdir(bdc_dir):
            if file_name.endswith('.csv') and not file_name.startswith('._'):
                file_path = os.path.join(bdc_dir, file_name)
                try:
                    df = pd.read_csv(file_path, nrows=1)
                    logging.debug(f"Columns in {file_name}: {df.columns.tolist()}")
                    if all(col in df.columns for col in required_columns):
                        valid_files.append(file_name)
                except Exception as e:
                    logging.error(f"Error reading file {file_path}: {e}")

        # Step 5: Select the latest version of duplicate files
        logging.debug("Step 5: Selecting the latest version of duplicate files")
        latest_files = {}
        for file_name in valid_files:
            match = pattern.match(file_name)
            if match:
                base_name = match.group(1) + match.group(2)
                date_str = match.group(3)
                date_obj = datetime.strptime(date_str, '%d%b%Y')
                if base_name not in latest_files or date_obj > latest_files[base_name][1]:
                    latest_files[base_name] = (file_name, date_obj)
        
        logging.debug("Final list of files to process:\n" + "\n".join([f"    {file_name}" for file_name, _ in latest_files.values()]))

        # Step 6: Concatenate all files into a DataFrame
        logging.debug("Step 6: Concatenating all files into a DataFrame")
        for base_name, (file_name, date_obj) in latest_files.items():
            file_path = os.path.join(bdc_dir, file_name)
            try:
                # Read CSV with specified dtypes
                df = pd.read_csv(file_path, dtype={
                    'frn': str,
                    'provider_id': str,
                    'brand_name': str,
                    'location_id': str,
                    'technology': str,
                    'block_geoid': str,
                    'h3_res8_id': str
                })
                
                # Format fields with consistent length and leading zeros
                df['block_geoid'] = df['block_geoid'].str.zfill(15)
                df['provider_id'] = df['provider_id'].str.zfill(6)
                df['location_id'] = df['location_id'].str.zfill(10)
                df['h3_res8_id'] = df['h3_res8_id'].str.ljust(15)
                
                # Map technology codes to abbreviations using dictionary
                df['technology_abbr'] = df['technology'].astype(int).map(tech_code_to_abbr)
 
                # Check for NaN values in technology mapping
                if df['technology_abbr'].isna().any():
                    logging.error("Found unmapped technology codes:")
                    unmapped = df[df['technology_abbr'].isna()]['technology'].unique()
                    logging.error(f"Unmapped codes: {unmapped}")

                # Map provider_id to holding_company using dictionary
                df['provider_name'] = df['provider_id'].apply(
                    lambda x: provider_id_to_name.get(int(x), str(x))
                )

                state_data.append(df)
                logging.debug(f"Reading file: {file_name}")
            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")

        if state_data:
            try:
                # Step 7: Concatenate the DataFrames
                state_df = pd.concat(state_data, ignore_index=True)
                logging.debug(f"Concatenated DataFrame columns: {state_df.columns.tolist()}")
                logging.debug("Sample of concatenated DataFrame:")
                logging.debug("\nFirst 5 rows of key columns:\n" + 
                            state_df[['block_geoid', 'technology_abbr', 'provider_name']].head().to_string())
                               
                # Define output path
                output_csv_path = os.path.join(state_dir, f"bdc_{fips_code_str}_{abbr}_BB.csv")
 
                # Step 8: Group by block_geoid and aggregate providers for each technology
                grouped = state_df.groupby('block_geoid')
                output_data = []

                for block_geoid, group in grouped:
                    row = {'block_geoid': block_geoid}
                    for tech_abbr in technology_code_df['Abbr'].str.strip():
                        providers = group[group['technology_abbr'] == tech_abbr]['provider_name'].unique().tolist()
                        row[f'{tech_abbr}_provider_name'] = providers if providers else []
                    output_data.append(row)

                output_df = pd.DataFrame(output_data)
                
                # Ensure all technology columns exist with empty lists as default
                for tech_abbr in technology_code_df['Abbr'].str.strip():
                    if f'{tech_abbr}_provider_name' not in output_df.columns:
                        output_df[f'{tech_abbr}_provider_name'] = output_df.apply(lambda x: [], axis=1)

                # Step 9: Add provider count columns for each technology
                for tech_abbr in technology_code_df['Abbr'].str.strip():
                    output_df[f'{tech_abbr}_provider_count'] = output_df[f'{tech_abbr}_provider_name'].apply(len)

                logging.debug(f"Step 9: DataFrame after adding technology columns and counts: {output_df.columns.tolist()}")
                logging.debug(f"Step 9: DataFrame after adding technology columns and counts:\n{output_df.head()}")

                # Create ordered column list
                columns = ['block_geoid']
                for tech_abbr in technology_code_df['Abbr'].str.strip():
                    columns.extend([
                        f'{tech_abbr}',  # provider names column
                        f'{tech_abbr}C'   # provider count column
                    ])
                                    
                # Reorder columns
                output_df = output_df[columns]
                
                logging.debug(f"DataFrame with ordered columns: {output_df.columns.tolist()}")
                logging.debug(f"DataFrame with ordered columns:\n{output_df.head()}")

                output_df.to_csv(output_csv_path, index=False)
                logging.debug(f"Output CSV file written: {output_csv_path}")

                # Step 10: Zip the output file
                logging.debug("Step 10: Zipping the output file")
                zip_file_path = output_csv_path.replace('.csv', '.zip')
                logging.debug(f"Zipping CSV file: {zip_file_path}")
                with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(output_csv_path, os.path.basename(output_csv_path))

                logging.debug(f"Removing temporary CSV file: {output_csv_path}")
                os.remove(output_csv_path)
            except Exception as e:
                logging.error(f"Error during aggregation or writing output for state {state_name}: {e}")

        # Ensure there are zipped versions for every .csv file in the bdc subdirectory and remove the .csv versions
        logging.debug("Ensuring there are zipped versions for every .csv file in the bdc subdirectory and removing the .csv versions")
        for file_name in os.listdir(bdc_dir):
            if file_name.endswith('.csv') and not file_name.startswith('._'):
                csv_file_path = os.path.join(bdc_dir, file_name)
                zip_file_path = csv_file_path.replace('.csv', '.zip')
                if not os.path.exists(zip_file_path):
                    logging.debug(f"Zipping CSV file: {zip_file_path}")
                    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                        zipf.write(csv_file_path, os.path.basename(csv_file_path))
                logging.debug(f"Removing CSV file: {csv_file_path}")
                os.remove(csv_file_path)

    logging.debug("State data aggregation completed")