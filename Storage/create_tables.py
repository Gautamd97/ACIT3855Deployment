# from sqlalchemy import create_engine
# from models import Base

# engine = create_engine("sqlite:///storage.db", echo=True, future=True)
# Base.metadata.create_all(engine)
# print("Tables created")


from database import ENGINE
from models import Base

Base.metadata.create_all(ENGINE)
print("Tables created.")