from dataclasses import dataclass

@dataclass
class ExtContext:
    """
    Context class to hold parsed information from Pentaho Kettle XML files.
    """
    query: str = None
    output_file_path: str = None

