import logging
from prepdf import create_technology_code_df, create_provider_df
from aggstatedata import aggregate_state_data
from fipscodes import states_and_territories

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Create the technology code DataFrame
technology_code_df = create_technology_code_df()

# Create the provider DataFrame
provider_df = create_provider_df()

# Call the aggregate_state_data function with the required arguments
aggregate_state_data(states_and_territories, technology_code_df, provider_df)