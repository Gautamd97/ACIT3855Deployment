from database import ENGINE
from models import Base

Base.metadata.create_all(ENGINE)
print("Tables created.")