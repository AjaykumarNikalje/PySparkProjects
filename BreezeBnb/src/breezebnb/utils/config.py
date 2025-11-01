import os
import yaml

class Config:
    def __init__(self, config_path):
        #  Check if config file exists
        if not os.path.exists(config_path):
            # Try to look one directory above (for src-based layout)
            alt_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "config.yaml")
            alt_path = os.path.abspath(alt_path)
            if os.path.exists(alt_path):
                config_path = alt_path
            else:
                raise FileNotFoundError(f" Config file not found at: {config_path} or {alt_path}")

        # Load YAML
        with open(config_path, "r") as file:
            cfg = yaml.safe_load(file)

        # Extract each section safely
        input_cfg = cfg.get("input", {})
        output_cfg = cfg.get("output", {})
        processing_cfg = cfg.get("processing", {})
        logging_cfg = cfg.get("logging", {})
        error_cfg = cfg.get("error", {})

        # Assign attributes
        self.input_file_name = input_cfg.get("input_file_name")
        self.input_path = input_cfg.get("input_path")
        self.input_file_extension=input_cfg.get("input_file_extension")

        self.output_path = output_cfg.get("output_path")
        self.avg_price_filename = output_cfg.get("avg_price_filename")
        self.topN_overpriced_filename = output_cfg.get("topNOverPricedListings")
        self.topN_underpriced_filename = output_cfg.get("topNUnderPricedListings")

        self.top_k = int(processing_cfg.get("top_k", 10))

        self.enable_file_log = logging_cfg.get("enable_file_log", True)
        self.log_dir = logging_cfg.get("log_dir", "logs/")
        self.level = logging_cfg.get("level", "INFO")
        self.log_file_name = logging_cfg.get("log_file_name", "batch")

        self.error_path = error_cfg.get("error_path", "resources/error/")
        self.error_file_name=error_cfg.get("error_file_name", "ErrorRecords")
