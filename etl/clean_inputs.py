import pandas as pd
from pathlib import Path


class CleaningInputs:

    # Set directory for mapping and order data
    mappingdir = Path.cwd().joinpath('etl', 'cleaning', 'mapping', 'api')
    orderdir = Path.cwd().joinpath('etl', 'cleaning', 'mapping', 'order')

    def __init__(self, round_dict):
        self.round_dict = round_dict


    def get_cleaning_inputs(self):
        cleaning_inputs_dict = self.combine_inputs()
        return cleaning_inputs_dict

    def combine_inputs(self):
        list_of_mappings = [self.generate_mapping_dict(svy_id) for svy_id in self.round_dict.values() if svy_id is not None]
        list_of_orders = [self.generate_order_list(svy_id) for svy_id in self.round_dict.values() if svy_id is not None]
        # Zip it all together
        cleaning_inputs = list(zip(list_of_mappings, list_of_orders))
        cleaning_inputs_dict = dict(zip(self.round_dict.values(), cleaning_inputs))
        return cleaning_inputs_dict

    # Create survey column mapping dict
    def generate_mapping_dict(self, svy_id):
        fn = svy_id + '_api_mapping' + '.csv'
        path = self.mappingdir.joinpath(fn)
        if path.is_file():
            output = pd.read_csv(self.mappingdir.joinpath(fn), header=0, index_col=0, squeeze=True).to_dict()
        else:
            print(f'Mapping file for {svy_id} DOES NOT EXIST!')
            output = None
        return output

    # Create survey column order list
    def generate_order_list(self, svy_id):
        fn = svy_id + '_col_order' + '.csv'
        path = self.orderdir.joinpath(fn)
        if path.is_file():
            output = pd.read_csv(path, header=None)[0].tolist()
        else:
            print(f'Order file for {svy_id} DOES NOT EXIST!')
            output = None
        return output

    



