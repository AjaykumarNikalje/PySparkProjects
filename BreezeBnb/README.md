# 🧠 **BreezeBnB Data Engineering Project**

A modular **PySpark project** built using **Poetry** for dependency management and packaging.  
This project demonstrates data ingestion, transformation, and reporting pipelines for Airbnb analytics.

---

## 🏗️ **Project Structure**

```
BreezeBnb/
├── README.md
├── pyproject.toml              # Poetry project configuration
├── poetry.lock                 # Poetry dependency lock file
├── dist/                       # Generated build artifacts (.whl, .tar.gz)
│   └── breezebnb-0.1.0-py3-none-any.whl
├── src/
│   └── breezebnb/
│       ├── __init__.py
│       ├── config/             # Configuration and constants
│       ├── data_access/        # Readers/Writers for data sources
│       ├── reporting/          # Reporting and analytics modules
│       ├── resources/          # Static resources or lookup files
│       ├── transformations/    # Spark transformation logic
│       └── utils/              # Logging, config loader, helpers
│
├── jobs/
│   ├── __init__.py
│   ├── config.yaml             # Job configuration
│   ├── run_airbnb_metrics.py   # ETL job for Airbnb metrics
│   └── run_analytics.py        # Analytics aggregation job
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── integration/
│   │   ├── test_data_reader_integration.py
│   │   ├── test_data_writer_integration.py
│   │   └── test_logger_integration.py
│   ├── test_airbnb_transformations.py
│   └── test_analytics_reporter.py
│
└── myenv/                      # Poetry-created virtual environment (optional local venv)
```

---

## ⚙️ **1️⃣ Create and Initialize the Project**

### Step 1 — Create the project folder
```bash
mkdir BreezeBnb
cd BreezeBnb

#Create Env
python3.11 -m venv myenv
source myenv/bin/activate
```





### Step 2 — Initialize Poetry
#If poetry is not installed, please install poetry first.
```bash
poetry init
```

Follow the prompts to define:
- Name: `breezebnb`
- Version: `0.1.0`
- Description: “A PySpark analytics project for Airbnb metrics.”
- License: “MIT” (or any)
- Dependencies: add none for now.

---

## 🧩 **2️⃣ Install Dependencies**
#If you are using existing folders then simply execute below command. This installs all dependencies mentioned in .toml file.
```bash
poetry install
```

### Add core dependencies:
#If you are installing dependencies one by one. This will add dependencies into the .toml file.
```bash
poetry add pyspark pyyaml
```


---

## 🧱 **3️⃣ Create the Source Folder Structure**

Poetry expects your package source under `src/`.  
So create:
```bash
mkdir -p src/breezebnb/{utils,reporting,transformations,data_access,config,resources}
touch src/breezebnb/__init__.py
```

Then create your **job scripts** (outside the package):
```bash
mkdir jobs
touch jobs/__init__.py
touch jobs/run_airbnb_metrics.py
touch jobs/run_analytics.py
```

Add your configuration file:
```bash
touch jobs/config.yaml
```

---

## 🧰 **4️⃣ Activate the Poetry Environment**

```bash
poetry shell
```

To confirm:
```bash
which python
```
It should point inside your Poetry virtualenv.

---

## 🧪 **5️⃣ Run Unit Tests**

```bash
pytest -v
```

If your imports fail, make sure your test runner is aware of the `src/` path:
```bash
PYTHONPATH=src pytest -v
```

---

## 📦 **6️⃣ Build the Project (Create Wheel)**

Once everything is working:
```bash
poetry build
```

This will create:
```
dist/
├── breezebnb-0.1.0-py3-none-any.whl
├── breezebnb-0.1.0.tar.gz
```

These are your **deployable artifacts** — ready for `spark-submit`.

---

## 🚀 **7️⃣ Run with Python**

You can now use `python` to execute your job.

From the **project root**:
```bash
python -m jobs.run_airbnb_metrics 2025-01 adhoc /Users/ajaykumarnikalje/Desktop/UKStudyProjects/PythonCode/BreezeBnb_Ver1_Test/PySparkProjects/BreezeBnb/jobs/config.yaml
```

---


```bash
python -m jobs.run_analytics 2025-01 ALL /Users/ajaykumarnikalje/Desktop/UKStudyProjects/PythonCode/BreezeBnb_Ver1_Test/PySparkProjects/BreezeBnb/jobs/config.yaml 5
```

---

## 📘 **1️⃣ Useful Poetry Commands**

| Task | Command |
|------|----------|
| Install dependencies | `poetry install` |
| Add new dependency | `poetry add package-name` |
| Build project | `poetry build` |
| Run script inside env | `poetry run python jobs/run_analytics.py` |
| Open shell | `poetry shell` |
| Show dependency tree | `poetry show --tree` |
| Check venv path | `poetry env info --path` |

---

## 🚀 **12️⃣ Example Full Flow**

```bash
# 1. Activate Poetry
poetry shell

# 2. Run tests
pytest -v

# 3. Build the wheel
poetry build

# 4. Run job with Spark

#Create one new folder "Testing" and add whl file dist/breezebnb-0.1.0-py3-none-any.whl and jobs folder. 
#Under Testing folder , run below commands.
spark-submit \
  --master "local[*]" \
  --py-files breezebnb-0.1.0-py3-none-any.whl \
  jobs/run_airbnb_metrics.py \
  2025-01 adhoc jobs/config.yaml

spark-submit \
  --master "local[*]" \
  --py-files breezebnb-0.1.0-py3-none-any.whl \
  jobs/run_analytics.py \
  2025-01 ALL jobs/config.yaml 10
```

---

## 🧭 **15️⃣ Summary**

| Step | Description |
|------|--------------|
| 1️⃣ | Create Poetry project with `src/` structure |
| 2️⃣ | Add dependencies (`pyspark`, `pytest`, etc.) |
| 3️⃣ | Build wheel with `poetry build` |
| 4️⃣ | Run Spark job with `spark-submit -m breezebnb.jobs.run_analytics` |
| 5️⃣ | Keep code modular (`utils/`, `transformations/`, `reporting/`) |
| 6️⃣ | Test using `pytest` and clean with `.gitignore` |

