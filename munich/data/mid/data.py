import numpy as np

"""
This stage provides some data provided in the MiD 2017 report for Munich
"""

def configure(context):
    pass

def execute(context):
    data = {}

    data["car_availability_constraints"] = [
        { "zone": "mr", "target": 0.47 },
        { "zone": "mvv", "target": 0.69 },
        { "zone": "umland", "target": 0.83 },
        { "zone": "munich", "target": 0.57 },
        #{ "zone": "mrs", "target": 0.62 },
        { "zone": "external", "target": 0.82 }, # Bavaria value
    ]

    data["bicycle_availability_constraints"] = [
        { "zone": "mvv", "target": 0.84 },
        { "zone": "munich", "target": 0.83 },
        { "zone": "umland", "target": 0.87 },
        { "zone": "mr", "target": 0.84 },
        #{ "zone": "mrs", "target": 0.83 },
        { "zone": "external", "target": 0.80 }, # Bavaria value

        { "zone": "munich", "sex": "male", "target": 0.85 },
        { "zone": "munich", "sex": "female", "target": 0.82 },

        { "zone": "munich", "age": (-np.inf, 17), "target": 0.92 },
        { "zone": "munich", "age": (18, 29), "target": 0.85 },
        { "zone": "munich", "age": (30, 49), "target": 0.90 },
        { "zone": "munich", "age": (50, 64), "target": 0.87 },
        { "zone": "munich", "age": (65, 74), "target": 0.76 },
        { "zone": "munich", "age": (75, np.inf), "target": 0.57 },

        # Umland
        { "zone": "umland", "sex": "male", "target": 0.88 },
        { "zone": "umland", "sex": "female", "target": 0.85 },
        
        { "zone": "umland", "age": (-np.inf, 17), "target": 0.96 },
        { "zone": "umland", "age": (18, 29), "target": 0.80 },
        { "zone": "umland", "age": (30, 49), "target": 0.90 },
        { "zone": "umland", "age": (50, 64), "target": 0.90 },
        { "zone": "umland", "age": (65, 74), "target": 0.85 },
        { "zone": "umland", "age": (75, np.inf), "target": 0.72 },
    ]

    data["pt_subscription_constraints"] = [
        { "zone": "mvv", "target": 0.35 },
        { "zone": "munich", "target": 0.47 },
        { "zone": "umland", "target": 0.22 },
        { "zone": "mr", "target": 0.51 },
        #{ "zone": "mrs", "target": 0.45 },
        { "zone": "external", "target": 0.17 }, # Bavaria value

        { "zone": "munich", "sex": "male", "target": 0.46 },
        { "zone": "munich", "sex": "female", "target": 0.50 },

        { "zone": "munich", "age": (-np.inf, 17), "target": 0.52 },
        { "zone": "munich", "age": (18, 29), "target": 0.65 },
        { "zone": "munich", "age": (30, 49), "target": 0.48 },
        { "zone": "munich", "age": (50, 64), "target": 0.40 },
        { "zone": "munich", "age": (65, 74), "target": 0.37 },
        { "zone": "munich", "age": (75, np.inf), "target": 0.34 },

        # Umland
        { "zone": "umland", "sex": "male", "target": 0.23 },
        { "zone": "umland", "sex": "female", "target": 0.21 },
        
        { "zone": "umland", "age": (-np.inf, 17), "target": 0.41 },
        { "zone": "umland", "age": (18, 29), "target": 0.39 },
        { "zone": "umland", "age": (30, 49), "target": 0.22 },
        { "zone": "umland", "age": (50, 64), "target": 0.20 },
        { "zone": "umland", "age": (65, 74), "target": 0.11 },
        { "zone": "umland", "age": (75, np.inf), "target": 0.11 },
    ]

    return data

