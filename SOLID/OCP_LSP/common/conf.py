def load_conf(path: str) -> dict:
    import yaml

    with open(path) as conf_file:
        conf_dict = yaml.safe_load(conf_file)

    return conf_dict