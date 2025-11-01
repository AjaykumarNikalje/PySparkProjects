from pathlib import Path
import yaml, os

class Config:
    def __init__(self, path: str):
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with p.open('r') as f:
            self._raw = yaml.safe_load(f)

    def get(self, *keys, default=None):
        node = self._raw
        for k in keys:
            if not isinstance(node, dict):
                return default
            node = node.get(k, default)
        return node

    @property
    def input_path(self):
        return Path(self.get('input', 'listings_csv'))

    @property
    def date_column(self):
        return self.get('input', 'date_column')

    @property
    def price_column(self):
        return self.get('input', 'price_column')

    @property
    def neighbourhood_column(self):
        return self.get('input', 'neighbourhood_column')

    @property
    def id_column(self):
        return self.get('input', 'id_column')

    @property
    def output_dir(self):
        return Path(self.get('output', 'output_dir'))

    @property
    def top_k(self):
        return int(self.get('processing', 'top_k', default=10))

    @property
    def spark_master(self):
        return self.get('spark', 'master', default=os.getenv('SPARK_MASTER', 'local[*]'))

    @property
    def spark_app_name(self):
        return self.get('spark', 'app_name', default='airbnb_price_pipeline')    


config=Config("config.yaml")
print(config.input_path)