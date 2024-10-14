import shutil
import os.path

import matsim.runtime.eqasim as eqasim
import matsim.simulation.prepare as delegate

def configure(context):
    delegate.configure(context)
    context.stage("munich.data.mvg.zones")

def execute(context):
    result = delegate.execute(context)

    df_zones = context.stage("munich.data.mvg.zones")
    df_zones.to_file("{}/transit_zones.shp".format(context.path()))

    eqasim.run(context, "org.eqasim.ile_de_france.munich.AddTransitZoneInformation", [
        "--input-path", "{}transit_schedule.xml.gz".format(context.config("output_prefix")),
        "--output-path", "{}transit_schedule.xml.gz".format(context.config("output_prefix")),
        "--zones-path", "transit_zones.shp"
    ])

    return result
