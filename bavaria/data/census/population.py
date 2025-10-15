import pandas as pd
import numpy as np
import os
from typing import Literal
from collections import defaultdict

root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))  # one level up -> pipeline-bavaria
csv_path = os.path.join(root, "age_distribution_2045.csv")
df_population_2040 = pd.read_csv(csv_path)

"""
This stage loads the raw census data for Bavaria.

TODO: This could be replaced with a Germany-wide extract from GENESIS
"""

def configure(context):
    context.stage("bavaria.data.spatial.codes")

    context.config("data_path")
    context.config("bavaria.population_path", "bavaria/a1310c_202200.xla")

def construct_municipality_id(municipality_code, association_code):
    if len(municipality_code) == 3:
        # a city without a Kreis, pad with zeros
        return "09" + municipality_code + "0000000" 
    elif len(municipality_code) == 6:
        # a regular Gemeinde with a Kreis

        if association_code == "-":
            # the Gemeinde is not in an association (Verbund)
            return "".join([
                "09", # Bavaria
                municipality_code[0:3], # First digit (Bezirk) + two digits (Kreis)
                "0", # indicating that it is not in a Verbund
                municipality_code[3:], # Repeat last three digits (Gemeinde)
                municipality_code[3:], # Repeat last three digits (Gemeinde)
            ])
        
        else:
            # the Gemeinde is in an association (Verbund)
            return "".join([
                "09", # Bavaria
                municipality_code[0:3], # First digit (Bezirk) + two digits (Kreis)
                "5", # indicating that it is in a Verbund
                str(association_code), # the association code
                municipality_code[3:], # Repeat last three digits (Gemeinde)
            ])

    raise RuntimeError("Invalid format")

def execute(context):
    df_census = pd.read_excel("{}/{}".format(
        context.config("data_path"), context.config("bavaria.population_path")
    ), sheet_name = "Gemeinden", skiprows = 5, names = [
        "municipality_code", "association_code", "name", "sex", "total", 
        "age_0", "age_3", "age_6","age_10", "age_15", "age_18", "age_20", "age_25", "age_30", "age_40", "age_50", "age_65", "age_75", 
        "municipality_code_copy", "association_code_copy"
    ])

    # Only keep rows where we have a value
    df_census = df_census[~df_census["total"].isna()].copy()
    
    # Padding of identifiers, only one following line
    df_census["municipality_code"] = df_census["municipality_code"].ffill(limit = 1)
    df_census["association_code"] = df_census["association_code"].ffill(limit = 1)
    
    # Only keep rows where we have 6 digits (Bezirk + Kreis + Gemeinde) or 3 digits (city without Kreis)
    df_census = df_census[
        (df_census["municipality_code"].str.len() == 6) | 
        (df_census["municipality_code"].str.len() == 3)
    ].copy()

    # Now reconstruct the municipality code (ARS, the first column gives the AGS!)
    # All municipalities that are without a Kreis get a 0 suffix
    df_census["commune_id"] = [
        construct_municipality_id(*codes) for codes in zip(
            df_census["municipality_code"], df_census["association_code"]
        ) 
    ]

    df_census["commune_id"] = df_census["commune_id"].astype("category")

    # Clean up age structure
    df_census = pd.melt(df_census, ["commune_id", "sex"], [
        "age_0", "age_3", "age_6","age_10", "age_15", "age_18", "age_20", "age_25", "age_30", "age_40", "age_50", "age_65", "age_75"
    ], var_name = "age_class", value_name = "population")

    df_census["age_class"] = df_census["age_class"].str.replace("age_", "").astype(int)

    # Clean counts
    df_census["population"] = df_census["population"].replace({ "-": 0 }).astype(int)

    # Cleanup gender
    df_census["sex"] = df_census["sex"].replace({
        "  insgesamt": "total", "  weiblich": "female"
    })

    df_census = pd.merge(
        df_census[df_census["sex"] == "total"].rename(columns = { "population": "total_population" }).drop(columns = ["sex"]),
        df_census[df_census["sex"] == "female"].rename(columns = { "population": "female_population" }).drop(columns = ["sex"]),
        on = ["commune_id", "age_class"]
    )
    
    df_census["male_population"] = df_census["total_population"] - df_census["female_population"]

    df_male = df_census[["commune_id", "age_class", "male_population"]].rename(columns = {
        "male_population": "weight"
    })
    df_male["sex"] = "male"

    df_female = df_census[["commune_id", "age_class", "female_population"]].rename(columns = {
        "female_population": "weight"
    })
    df_female["sex"] = "female"

    df_census = pd.concat([df_male, df_female])
    df_census["sex"] = df_census["sex"].astype("category")

    # Filter for requested codes
    df_codes = context.stage("bavaria.data.spatial.codes")
    df_census = df_census[df_census["commune_id"].isin(df_codes["commune_id"])]
    
    # Adapt here for population 2040. München has the code "62", and is in Oberbayern (091). Therefore, the correpsonding commune_id is "91620000000".

    # Keep a copy of original weights for verification (so we can show before/after)
    df_census_2040 = df_census.copy()
    df_census_2040['_orig_weight'] = df_census_2040['weight']
    
    # For convenience, ensure population age bounds are integers
    df_population_2040['age_start'] = df_population_2040['age_start'].astype(int)
    df_population_2040['age_end']   = df_population_2040['age_end'].astype(int)

    # Call prechecks
    target_communes = ["091620000000"]  # adapt as needed (list of strings)
    pre_msgs, df_population_2040_checked, df_census_2040 = run_prechecks(df_population_2040, df_census_2040, target_communes)
    for m in pre_msgs:
        print(m)
        
    # -------------------------
    # Prepare original snapshot and types
    # -------------------------
    if '_orig_weight' not in df_census_2040.columns:
        df_census_2040['_orig_weight'] = df_census_2040['weight']

    weight_col = 'weight'
    orig_col  = '_orig_weight'

    # Keep dtype info to restore/cast if necessary
    orig_weight_dtype = df_census_2040[orig_col].dtype
    is_nullable_int = str(orig_weight_dtype).startswith('Int') or pd.api.types.is_integer_dtype(df_census_2040[orig_col])
    is_float = pd.api.types.is_float_dtype(df_census_2040[orig_col])

    # -------------------------
    # Main loop: iterate rows and update by positional index
    # -------------------------
    updates = []
    n_rows = len(df_census_2040)
    col_pos = df_census_2040.columns.get_loc(weight_col)

    for pos, (index_label, row) in enumerate(df_census_2040.iterrows()):
        # Note: enumerate ensures pos is the 0-based physical row index in this loop order.
        commune = str(row.get('commune_id', '')).strip()
        if commune not in target_communes:
            continue

        age_val = row.get('age_class', None)
        age_int = safe_int(age_val)
        if age_int is None:
            # skip and optionally record
            continue

        pop_idx = find_population_row_index_for_age(age_int, df_population_2040_checked)
        if pop_idx is None:
            continue

        sex_norm = normalize_sex_label(row.get('sex', None))
        if sex_norm is None:
            continue

        # pick new weight from population df (already coerced to numeric)
        if sex_norm == 'male':
            new_weight_raw = df_population_2040_checked.at[pop_idx, 'male_2045']
        else:
            new_weight_raw = df_population_2040_checked.at[pop_idx, 'female_2045']

        # cast to match original dtype if reasonable
        new_weight_cast = new_weight_raw
        try:
            if is_nullable_int:
                # round then cast to python int (pandas will accept when placing into column)
                new_weight_cast = int(round(float(new_weight_raw)))
            elif is_float:
                new_weight_cast = float(new_weight_raw)
            else:
                # leave as-is (likely object or other)
                new_weight_cast = new_weight_raw
        except Exception as e:
            # fallback: leave as raw numeric, but note in updates
            new_weight_cast = new_weight_raw

        # Write via .iat to the physical row position
        df_census_2040.iat[pos, col_pos] = new_weight_cast

        updates.append({
            'pos': pos,
            'index_label': index_label,
            'commune_id': commune,
            'age_class': age_int,
            'sex': sex_norm,
            'old_weight': row.get(weight_col),
            'new_weight': new_weight_cast,
            'pop_idx': int(pop_idx),
            'pop_range': (int(df_population_2040_checked.at[pop_idx,'age_start']),
                        int(df_population_2040_checked.at[pop_idx,'age_end']))
        })

    # -------------------------
    # Post-checks & summary
    # -------------------------
    updates_df = pd.DataFrame(updates)
    print("Total census rows:", n_rows)
    print("Target communes:", target_communes)
    print("Rows updated:", len(updates_df))
    if not updates_df.empty:
        print("Sample of updates (first 200):")
        print(updates_df[['pos','index_label','commune_id','age_class','sex','old_weight','new_weight','pop_range']].to_string(index=False))

    # Ensure rows outside targets didn't change
    others = df_census_2040.loc[~df_census_2040['commune_id'].isin(target_communes)]
    outside_changed = (others['weight'] != others[orig_col]).any()
    print("Any outside-target rows changed?", bool(outside_changed))

    # Optional: show dtype summary
    print("Data types (df_population_2040):")
    print(df_population_2040_checked[['age_start','age_end','male_2045','female_2045']].dtypes)
    print("Data types (df_census_2040):")
    print(df_census_2040.dtypes)
    
    raise ValueError("Stop here")
    
    df_census_output = df_census[["commune_id", "sex", "age_class", "weight"]]
    df_census_output.to_csv("census_data.csv", index=False)
    print("Census data saved to census_data.csv")
    return df_census_output

    # return df_census[["commune_id", "sex", "age_class", "weight"]]

# --- Safety diagnostics before mutating ---
def run_prechecks(df_pop, df_census, target_communes):
    msgs = []
    # dtype checks for population df
    required_pop_cols = {'age_start','age_end','male_2045','female_2045'}
    missing = required_pop_cols - set(df_pop.columns)
    if missing:
        raise ValueError(f"df_population_2040 missing columns: {missing}")

    # Coerce age_start/age_end to integers for comparisons (but keep original copy)
    try:
        df_pop['age_start_check'] = pd.to_numeric(df_pop['age_start'], errors='coerce').astype('Int64')
        df_pop['age_end_check']   = pd.to_numeric(df_pop['age_end'], errors='coerce').astype('Int64')
    except Exception as e:
        raise ValueError("Unable to coerce age_start/age_end to integer-like values: " + str(e))

    if df_pop['age_start_check'].isna().any() or df_pop['age_end_check'].isna().any():
        raise ValueError("Some age_start or age_end values could not be parsed as integers. Please check df_population_2040.")

    # check age_start <= age_end
    bad_ranges = df_pop[df_pop['age_start_check'] > df_pop['age_end_check']]
    if not bad_ranges.empty:
        raise ValueError("Found population rows where age_start > age_end. Inspect: \n" + bad_ranges.to_string())

    # check for overlapping ranges (simple O(n^2) check - ok for small lists)
    overlaps = []
    pop_rows = df_pop[['age_start_check','age_end_check']].reset_index()
    for i, r1 in pop_rows.iterrows():
        for j, r2 in pop_rows.iterrows():
            if i >= j:
                continue
            if (r1['age_start_check'] <= r2['age_end_check']) and (r2['age_start_check'] <= r1['age_end_check']):
                overlaps.append((int(r1['index']), int(r2['index']),
                                int(r1['age_start_check']), int(r1['age_end_check']),
                                int(r2['age_start_check']), int(r2['age_end_check'])))
    if overlaps:
        msgs.append("Warning: overlapping population age ranges detected (pairs of row indices and ranges):")
        for ov in overlaps:
            msgs.append(str(ov))

    # ensure male_2045 and female_2045 numeric
    for c in ['male_2045','female_2045']:
        coerced = pd.to_numeric(df_pop[c], errors='coerce')
        na_count = int(coerced.isna().sum())
        if na_count > 0:
            raise ValueError(f"Column {c} has {na_count} non-numeric values after coercion.")
        # store back numeric series so later code uses numeric values
        df_pop[c] = coerced

    # census df checks
    required_census_cols = {'commune_id','age_class','weight','sex'}
    missing2 = required_census_cols - set(df_census.columns)
    if missing2:
        raise ValueError(f"df_census_2040 missing columns: {missing2}")

    # commune_id as string
    df_census['commune_id'] = df_census['commune_id'].astype(str)

    # check duplicate indices — not fatal, but warn
    dup_idx = df_census.index[df_census.index.duplicated()].unique()
    if len(dup_idx):
        msgs.append(f"Warning: DataFrame has {len(dup_idx)} duplicated index labels (not fatal). Examples: {list(dup_idx[:5])}")

    # return any warning messages and the possibly-updated dataframes
    return msgs, df_pop, df_census

def safe_int(x):
    if pd.isna(x):
        return None
    if isinstance(x, (pd.Series, np.ndarray, list)):
        try:
            if isinstance(x, pd.Series):
                x = x.dropna().iloc[0] if len(x.dropna()) else x.iloc[0]
            else:
                x = x[0]
        except Exception:
            return None
    try:
        return int(x)
    except (TypeError, ValueError):
        return None

def normalize_sex_label(s):
    """
    Normalize a single scalar sex value.
    Returns 'male' or 'female' or None.
    """
    if pd.isna(s):
        return None

    # Convert pandas Categorical to string
    if isinstance(s, pd.Categorical):
        s_str = str(s)
    else:
        s_str = str(s)

    s_str = s_str.strip().lower()
    if s_str == 'male':
        return 'male'
    elif s_str == 'female':
        return 'female'
    else:
        return None

# Build a small helper: given an age_class, return the row index (or None) in df_population_2040 whose range contains the age
    # We'll rely on the order of df_population_2040 for deterministic behavior.
def find_population_row_index_for_age(age, df_pop):
    mask = (df_pop['age_start'] == age) 
    matches = df_pop.index[mask]
    if len(matches) == 0:
        return None
    else:
        return matches[0]   # pick the first match if multiple (shouldn't happen with non-overlapping ranges)

def validate(context):
    if not os.path.exists("{}/{}".format(context.config("data_path"), context.config("bavaria.population_path"))):
        raise RuntimeError("Bavarian census data is not available")

    return os.path.getsize("{}/{}".format(context.config("data_path"), context.config("bavaria.population_path")))
