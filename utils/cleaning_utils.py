import pandas as pd

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

def parse_and_set_session_column(df):
    for col in df.columns:
        if 'time' in col or 'date' in col:
            try:
                df[col] = pd.to_datetime(df[col], format='%d.%m.%Y %H:%M:%S', errors='coerce')
            except Exception:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            if df[col].notna().sum() > 0:
                df.rename(columns={col: 'session'}, inplace=True)
                break
    if 'session' in df.columns:
        df.sort_values(by='session', inplace=True)
    return df

def convert_numeric_columns(df):
    for col in df.columns:
        if col != 'session':
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                continue
    return df

def compare_headers(expected, actual):
    return set(expected) == set(actual)
