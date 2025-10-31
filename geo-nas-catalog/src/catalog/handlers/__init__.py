"""File handlers for the catalog scanner."""

from .base import FileHandler
from .netcdf import NetCDFHandler
from .other import GenericHandler
from .tabular import CSVHandler, ExcelHandler

__all__ = [
    "FileHandler",
    "NetCDFHandler",
    "GenericHandler",
    "CSVHandler",
    "ExcelHandler",
]
