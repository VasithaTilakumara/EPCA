import json
import logging

import pandas as pd

from utils.create_glue_tables import type_mapping


def standardize_headers(df):
    df.columns = [str(col).strip().lower().replace(" ", "_") for col in df.columns]
    df = df.loc[:, (df.columns.notnull()) & (df.columns != '')]
    df = df.loc[:, ~df.columns.str.contains('^unnamed', case=False)]
    return df

def drop_empty_rows_cols(df):
    df.dropna(how='all', inplace=True)
    df.dropna(axis=1, how='all', inplace=True)
    return df

def drop_duplicate_rows(df):
    df.drop_duplicates(inplace=True)
    return df

def remove_null_timestamps(df):
    if 'time' in df.columns:
        df = df[~df['time'].isnull() & (df['time'] != '0') & (df['time'] != 0)]
    return df

def parse_and_set_datetime_column(df):
    for col in df.columns:
        if 'time' in col or 'date' in col:
            try:
                df[col] = pd.to_datetime(df[col], format='%d.%m.%Y %H:%M:%S', errors='coerce')
            except Exception:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            if df[col].notna().sum() > 0:
                df.rename(columns={col: 'datetime'}, inplace=True)
                df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')  # <- 👈 Force dtype here
                break
    if 'datetime' in df.columns:
        df.sort_values(by='datetime', inplace=True)
    return df

def convert_numeric_columns(df):
    for col in df.columns:
        if col != 'datetime':
            try:
                df[col] = pd.to_numeric(df[col])
                # if col == "shutdownsafe" and df[col].dtype == object:
                #     df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception:
                continue
    return df

def compare_headers(expected, actual):
    return set(expected) == set(actual)


def assign_dtypes(df, module_name):
    """
    Assigns dtypes to the dataframe based on the schema for the given module.

    Args:
        df (pd.DataFrame): Cleaned dataframe with standardized headers
        module_name (str): Module name (e.g., 'vehicle', 'battery1')

    Returns:
        pd.DataFrame: DataFrame with corrected dtypes
    """
    # Load your JSON schema once
    with open("utils/sampled_schema_summary.json", "r") as f:
        SCHEMA_TYPES = json.load(f)

    print(f"Assigning dtypes to '{module_name}'...")
    if module_name not in SCHEMA_TYPES:
        raise ValueError(f"Module '{module_name}' not found in schema registry.")

    module_schema = SCHEMA_TYPES[module_name]

    # Only apply dtype conversion to columns that exist in both schema and df
    common_cols = set(df.columns).intersection(module_schema.keys())

    for col in common_cols:
        dtype = module_schema[col]

        try:
            if dtype.startswith("float"):
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif dtype.startswith("int"):
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')
            elif dtype == "object":
                df[col] = df[col].astype(str)
            else:
                df[col] = df[col].astype(dtype)
        except Exception as e:
            print(f"Warning: Failed to convert {col} to {dtype}: {e}")

    return df
