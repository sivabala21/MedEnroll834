import logging
logging.disable(logging.CRITICAL)
import warnings
warnings.filterwarnings("ignore")

try:
    from airflow.dag_processing.dagbag import DagBag
except Exception:
    from airflow.models.dagbag import DagBag

db = DagBag("/opt/airflow/dags")
with open("/tmp/dagresult.txt", "w") as f:
    f.write("IMPORT_ERRORS: " + repr(db.import_errors) + "\n")
    f.write("DAGS: " + repr(list(db.dags.keys())) + "\n")
