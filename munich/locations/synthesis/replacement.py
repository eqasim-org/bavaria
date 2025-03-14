import synthesis.population.spatial.primary.locations as base

"""
This step replaces synthesis.population.spatial.primary.locations. It basically applies the
same logic but then overrides the education locations with the new ones from the Bavaria
pipeline.
"""

def configure(context):
    # Copy & past from base
    context.stage("synthesis.population.spatial.primary.candidates")
    context.stage("synthesis.population.spatial.commute_distance")
    context.stage("synthesis.population.spatial.home.locations")
    context.stage("synthesis.locations.work")
    context.stage("synthesis.locations.education")

    # Custom data
    context.stage("bavaria.locations.synthesis.education")

def execute(context):
    # We delegate the logic to the base step
    df_work, df_original = base.execute(context)

    # And we override the education decisions
    df_replacement = context.stage("bavaria.locations.synthesis.education")

    # Verification
    assert len(df_original) == len(df_replacement)
    assert set(df_original["person_id"]) == set(df_replacement["person_id"])

    return df_work, df_replacement
