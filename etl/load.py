# Load data into postgreSQL database
import sqlalchemy as db
from models import Base, LoadFiji
from sqlalchemy.orm import sessionmaker

# Analytical helper functions
from aw_analytics import mean_wt, median_wt, output_mean_tableau

# Value to switch between development and production
ENV = 'dev'

if ENV == 'dev':
    # development database
    DATABASE_URI = 'postgresql://postgres:P@ssw1rd@localhost/mvam'
else:
    # deployment database
    pass

engine = db.create_engine(DATABASE_URI)

Session = sessionmaker(bind=engine)

def recreate_database():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)



if __name__ == "__main__":
    recreate_database()
    s = Session()
    # test = LoadFiji(Round='Round_1', Demograph='Round', Demograph_Value='Round_1',
    # Indicator='FCG_Acceptable', Indicator_Value=39.0, Weighted_Count=409.6)
    s.add(test)
    s.commit()
    s.close()



