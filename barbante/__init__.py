""" Barbante library documentation.
"""
from barbante.utils import resource_stream

# Use the generated VERSION file to get the package version
VERSION_FILENAME = 'VERSION'
with resource_stream(VERSION_FILENAME) as version_file:
    __version__ = version_file.read().strip().decode("utf-8")

__app_name__ = "barbante " + __version__
