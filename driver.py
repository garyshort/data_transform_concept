import importlib
import os
import sys
import logging

import pandas as pd

# Create a dictionary for holding all the tables and their transforms
tables = {}


def build_table_delegates():
    """
    Search a given directory for all table transform files.
    When found, import any transforms and and place them in 
    the tables dictionary, keyed on the table the are to
    transform
    """
    for filename in os.listdir(os.getcwd()):
        if filename.startswith('db') & filename.endswith(".py"):
            key = filename.split(".")[0]
            tables[key] = {
                "transforms": importlib.import_module(key).transforms}
            logging.info("Transforms imported for table: " + key)


def transform_column_in_table(table, column, value):
    # Apply a given transform
    if column in tables[table]["transforms"]:
        logging.info("Transforming value: %s, of column: %s, in table: %s"
                     % (value, column, table))

        return tables[table]["transforms"][column](value)


def apply_transforms(table, df_new, df_raw):
    """
    For every cell in the raw table matrix, test if 
    there is a transform for it, in if there is then
    apply the transform. Return the transformed 
    data frame
    """
    row_indexes = range(0, len(df_new))
    col_names = list(df_new.columns)
    for row_index in row_indexes:
        for col_name in col_names:
            value = df_raw.iloc[row_index][col_name]
            transformed_value = transform_column_in_table(
                table, col_name, value)
            df_new.at[row_index, col_name] = transformed_value
    return df_new


def init():
    # Initialise the application
    logging.basicConfig(filename='transformer.log', level=logging.DEBUG)


def main():
    """
    The main function is the entry point and
    drives all of the table transforms
    """

    init()

    try:
        build_table_delegates()

        for table in tables:
            df_raw = pd.read_csv(table + ".csv", header='infer')
            df_new = df_raw.copy(deep=False)
            df_new = apply_transforms(table, df_new, df_raw)
            df_new.to_csv(table + "_transformed.csv", index=False,)
            logging.info(table + "_transformed.csv written")
            return 0
    except:
        logging.warning("Unexpected error: " + sys.exc_info()[0])
        return 1


main()
