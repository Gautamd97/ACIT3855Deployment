from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml

with open("/app/config/app_conf.yml", "r") as f:
    APP_CONF = yaml.safe_load(f.read())

db_conf = APP_CONF["datastore"]

DB_ENGINE_STRING = (
    f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}"
    f"@{db_conf['hostname']}:{db_conf['port']}/{db_conf['db']}"
)

ENGINE = create_engine(DB_ENGINE_STRING, echo=False, future=True)

def make_session():
    return sessionmaker(bind=ENGINE, autoflush=False, autocommit=False, future=True)()