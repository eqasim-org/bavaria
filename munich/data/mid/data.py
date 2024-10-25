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
        { "zone": "bavaria", "target": 0.82 },
    ]

    data["bicycle_availability_constraints"] = [
        # ignoring Umland to cover the rest
        { "zone": "!munich", "sex": "male", "target": 0.88 },
        { "zone": "!munich", "sex": "female", "target": 0.85 },
        
        { "zone": "!munich", "age": (-np.inf, 17), "target": 0.96 },
        { "zone": "!munich", "age": (18, 29), "target": 0.80 },
        { "zone": "!munich", "age": (30, 49), "target": 0.90 },
        { "zone": "!munich", "age": (50, 64), "target": 0.90 },
        { "zone": "!munich", "age": (65, 74), "target": 0.85 },
        { "zone": "!munich", "age": (75, np.inf), "target": 0.72 },

        { "zone": "mvv", "target": 0.84 },
        { "zone": "munich", "target": 0.83 },
        { "zone": "umland", "target": 0.87 },
        { "zone": "mr", "target": 0.84 },
        #{ "zone": "mrs", "target": 0.83 },
        #{ "zone": "bavaria", "target": 0.80 },

        { "zone": "munich", "sex": "male", "target": 0.85 },
        { "zone": "munich", "sex": "female", "target": 0.82 },

        { "zone": "munich", "age": (-np.inf, 17), "target": 0.92 },
        { "zone": "munich", "age": (18, 29), "target": 0.85 },
        { "zone": "munich", "age": (30, 49), "target": 0.90 },
        { "zone": "munich", "age": (50, 64), "target": 0.87 },
        { "zone": "munich", "age": (65, 74), "target": 0.76 },
        { "zone": "munich", "age": (75, np.inf), "target": 0.57 },
    ]

    data["pt_subscription_constraints"] = [
        # ignoring Umland to cover the rest
        { "zone": "!munich", "sex": "male", "target": 0.23 },
        { "zone": "!munich", "sex": "female", "target": 0.21 },
        
        { "zone": "!munich", "age": (-np.inf, 17), "target": 0.41 },
        { "zone": "!munich", "age": (18, 29), "target": 0.39 },
        { "zone": "!munich", "age": (30, 49), "target": 0.22 },
        { "zone": "!munich", "age": (50, 64), "target": 0.20 },
        { "zone": "!munich", "age": (65, 74), "target": 0.11 },
        { "zone": "!munich", "age": (75, np.inf), "target": 0.11 },

        { "zone": "mvv", "target": 0.35 },
        { "zone": "munich", "target": 0.47 },
        { "zone": "umland", "target": 0.22 },
        { "zone": "mr", "target": 0.51 },
        #{ "zone": "mrs", "target": 0.45 },
        #{ "zone": "bavaria", "target": 0.17 },

        { "zone": "munich", "sex": "male", "target": 0.46 },
        { "zone": "munich", "sex": "female", "target": 0.50 },

        { "zone": "munich", "age": (-np.inf, 17), "target": 0.52 },
        { "zone": "munich", "age": (18, 29), "target": 0.65 },
        { "zone": "munich", "age": (30, 49), "target": 0.48 },
        { "zone": "munich", "age": (50, 64), "target": 0.40 },
        { "zone": "munich", "age": (65, 74), "target": 0.37 },
        { "zone": "munich", "age": (75, np.inf), "target": 0.34 },
    ]

    return data

