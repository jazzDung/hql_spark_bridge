import re
import xml.etree.ElementTree as ET
from pathlib import Path

import sqlglot
import yaml
from context.ext_context import ExtContext
from paths import EXT_VARIABLE_CONFIG_PATH, EXT_CONFIG_PATH, FILE_CONVERT_CONFIG_PATH
import configparser

from utils.file_utils import parse_file_name, read_conf_to_dict


class ExtParser:
    """
    Parses Pentaho Kettle XML files to extract specific information.
    """

    def __init__(self, variable_mapping: dict = None, source_db_config: dict = None, file_convert_config: dict = None):
        """
        Receives mapping rules from the variable.yaml file
        Example: {'raw_schema': 'params["raw_schema"]', 'batch_date': 'batch_date'}
        """

        if variable_mapping is None:
            # 2. Load mapping configuration from YAML
            with open(EXT_VARIABLE_CONFIG_PATH, 'r', encoding='utf-8') as f:
                self.variable_mapping = yaml.safe_load(f)
        else:
            self.variable_mapping = variable_mapping

        if source_db_config is None:
            # 1. Initialize the parser
            config = configparser.ConfigParser()

            # 2. Read the file
            config.read(EXT_CONFIG_PATH)

            self.source_db_config = {s: dict(config.items(s)) for s in config.sections()}
        else:
            self.source_db_config = source_db_config

        if file_convert_config is None:
            self.file_convert_config = read_conf_to_dict(FILE_CONVERT_CONFIG_PATH)
        else:
            self.file_convert_config = file_convert_config

    def parse_ext(self, file_path: Path) -> ExtContext:
        """
        Parses the given Pentaho Kettle XML file and extracts the SQL query
        from the 'Table input' step and the output file path from the 'Text file output' step.

        Args:
            file_path: The path to the Pentaho Kettle XML file.

        Returns:
            An ExtContext object containing the extracted query and output file path.
        """


        layer, sub_layer, source_name, base_table = parse_file_name(file_path)
        source_db_config = self.source_db_config.get(source_name, {})
        original_file_path = self.file_convert_config.get(f"{source_name}_{base_table}")
        read_from_text_file = f"{source_name}_{base_table}" in self.source_db_config['excute_exception']


        if not file_path.exists():
            if read_from_text_file:
                print(f"Warning: The file {file_path} does not exist since it is in the list of exceptions.")
                return ExtContext(
                    source=source_name,
                    read_from_text_file=True,
                    original_file_path=original_file_path,
                    query=None,
                    output_file_path=None,
                    variable_mapping=self.variable_mapping,
                    source_db_config=source_db_config
                )

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

        return ExtContext(
            source=source_name,
            read_from_text_file=read_from_text_file,
            query=query,
            output_file_path=output_file_path,
            variable_mapping=self.variable_mapping,
            source_db_config=source_db_config
        )