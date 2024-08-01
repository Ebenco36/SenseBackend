from abc import ABC, abstractmethod
from sqlalchemy.orm import sessionmaker

class Service(ABC):
    @abstractmethod
    def authenticate(self):
        pass

    @abstractmethod
    def fetch(self):
        pass
    

def query_data(engine, model):
    Session = sessionmaker(bind=engine)
    session = Session()

    rows = session.query(model).all()
    for row in rows:
        print(row.__dict__)  # Print all columns for each row
