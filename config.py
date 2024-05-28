import os
from configparser import ConfigParser

def load_config(section:str):
    parser = ConfigParser()
    base_dir = os.path.join(os.path.dirname(__file__))
    config_file_name = "config.ini"
    path_to_config_file = os.path.join(base_dir, config_file_name)
    parser.read(path_to_config_file)
    config = {}
    
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, config_file_name))

    return config

if __name__ == '__main__':
    config = load_config(section='postgresql')
    print(config)