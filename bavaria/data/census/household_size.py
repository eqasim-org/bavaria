import pandas as pd
import numpy as np

"""
This stage loads and processes household size data for Bavaria.

The module reads household size data from Excel files and formats it for use
in the synthetic population pipeline. It handles German gender classifications
and converts them to standardized categories.
"""

def configure(context):
    """
    Configure the stage by declaring configuration parameters.

    Args:
        context: The context object for the pipeline stage
    """
    context.config("data_path")
    context.config("bavaria.household_size_path", "bavaria/12211-105.xlsx")

def execute(context):
    """
    Execute the stage to process household size data.

    Args:
        context: The context object containing the pipeline data

    Returns:
        pd.DataFrame: Processed household size data with weights by household size and gender
    """
    # Load data
    df = pd.read_excel("{}/{}".format(context.config("data_path"), context.config("bavaria.household_size_path")), 
        sheet_name = "Tab2", skiprows = 5)

    # Clean up
    df = df[["Bevölkerung nach Haushaltsgröße und Geschlecht in Bayern"][1:]:]

    # Clean column names
    columns = list(df.columns)
    columns[0] = "household_size"
    df.columns = columns

    # Clean values
    df = df[df["household_size"] != "ausgewählt"]
    df["household_size"] = df["household_size"].replace({
        "1 Person": "1", "2 Personen": "2", "3 Personen": "3", 
        "4 Personen": "4", "5 Personen und mehr": "5+"
    }).astype(str)

    # Reshape data
    df = pd.melt(df, id_vars = ["household_size"], var_name = "sex", value_name = "weight")

    # Clean weights
    df["weight"] = df["weight"].fillna(0).astype(float)

    # Clean gender categories
    df = df[df["sex"].eq("weiblich") | df["sex"].eq("männlich")]
    df["sex"] = df["sex"].replace({
        "männlich": "male", "weiblich": "female"
    }).astype("category")

    return df[["household_size", "sex", "weight"]]

def validate(context):
    """
    Validate that the household size data file exists and is accessible.

    Args:
        context: The context object containing configuration data

    Returns:
        int: Size of the household size data file

    Raises:
        RuntimeError: If the household size data file is not found
    """
    import os
    if not os.path.exists("{}/{}".format(context.config("data_path"), context.config("bavaria.household_size_path"))):
        raise RuntimeError("Bavarian household size data is not available")

    return os.path.getsize("{}/{}".format(context.config("data_path"), context.config("bavaria.household_size_path")))
