from database import ENGINE
from models import Base

Base.metadata.drop_all(ENGINE)
print("Tables Dropped")