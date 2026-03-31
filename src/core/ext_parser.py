import xml.etree.ElementTree as ET
from pathlib import Path
from context.ext_context import ExtContext




class ExtParser:
    """
    Parses Pentaho Kettle XML files to extract specific information.
    """

    @staticmethod
    def parse_ext(file_path: Path) -> ExtContext:
        """
        Parses the given Pentaho Kettle XML file and extracts the SQL query
        from the 'Table input' step and the output file path from the 'Text file output' step.

        Args:
            file_path: The path to the Pentaho Kettle XML file.

        Returns:
            An ExtContext object containing the extracted query and output file path.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"The file {file_path} does not exist.")

        tree = ET.parse(file_path)
        root = tree.getroot()

        query = None
        output_file_path = None

        # 1. Parse the SQL query from the <step> with <name>Table input</name>
        for step in root.findall(".//step"):
            name_element = step.find("name")
            if name_element is not None and name_element.text == "Table input":
                sql_element = step.find("sql")
                if sql_element is not None:
                    query = sql_element.text.strip()
                break

        # 2. Parse the output file path from the <step> with <name>Text file output</name>
        for step in root.findall(".//step"):
            name_element = step.find("name")
            if name_element is not None and name_element.text == "Text file output":
                file_element = step.find("file")
                if file_element is not None:
                    name_path_element = file_element.find("name")
                    if name_path_element is not None:
                        output_file_path = name_path_element.text.strip()
                break

        return ExtContext(query=query, output_file_path=output_file_path)
