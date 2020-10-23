
import cleaning.fiji_r1_cleaning as f1cl
import cleaning.samoa_r1_cleaning as s1cl

class CleanData:
    
    def __init__(self, df_dict, col_mapping_dict, col_order_list):
        self.df_dict = df_dict
        self.col_mapping_dict = col_mapping_dict
        self.col_order_list = col_order_list
        self.fiji_r1_df = self.df_dict['FIJI_R1']
        self.samoa_r1_df = self.df_dict['SAMOA_R1']

    def clean_fiji_data(self):
        # Round 1
        f1cl.fiji_r1_clean_data(f1cl.fiji_r1_preprocess_data(self.fiji_r1_df, self.col_mapping_dict, self.col_order_list))
        print('Fiji R1 cleaning complete!')
        # Round 2

    def clean_samoa_data(self):
        # Round 1
        s1cl.samoa_r1_clean_data(s1cl.samoa_r1_preprocess_data(self.samoa_r1_df, self.col_mapping_dict, self.col_order_list))
        print('Samoa R1 cleaning complete!')

    def clean_tonga_data(self):
        pass
        # Round 1
