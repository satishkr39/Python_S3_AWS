import yaml
from yaml.loader import SafeLoader

with open('config.yaml') as f:
    data = yaml.load(f, Loader=SafeLoader)
    print(data)

print(data['config_file_path'].replace("\\","\\\\"))