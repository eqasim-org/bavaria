import subprocess as sp
import shutil, os

def configure(context):
    context.config("osmconvert_binary", "osmconvert")

def run(context, arguments = [], cwd = None):
    """
        This function calls osmconvert.
    """
    # Make sure there is a dependency
    # context.stage("data.osm.osmconvert")

    if cwd is None:
        cwd = context.path()

    # Prepare command line
    command_line = [
        shutil.which(context.config("osmconvert_binary"))
    ] + arguments

    # Run osmconvert
    return_code = sp.check_call(command_line, cwd = cwd)

    if not return_code == 0:
        raise RuntimeError("osmconvert return code: %d" % return_code)

def validate(context):
    if shutil.which(context.config("osmconvert_binary")) in ["", None]:
        raise RuntimeError("Cannot find osmconvert binary at: %s" % context.config("osmconvert_binary"))

    if not b"0.8." in sp.check_output([
        shutil.which(context.config("osmconvert_binary")),
        "-v"
    ], stderr = sp.STDOUT):
        print("WARNING! osmconvert of at least version 0.8.x is recommended!")

def execute(context):
    pass
