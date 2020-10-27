from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float

Base = declarative_base()

class LoadFiji(Base):
    __tablename__ = 'fiji'
    id = Column(Integer, primary_key=True)
    Round = Column(String)
    Demograph = Column(String)
    Demograph_Value = Column(String)
    Indicator = Column(String)
    Indicator_Value = Column(Float)
    Weighted_Count = Column(Float)

    def __repr__(self):
        return f"<LoadPacificMVAM(Round={self.Round}, Demograph_Value={self.Demograph_Value},\
            Indicator={self.Indicator}, Indicator_Value={self.Indicator_Value}>)"

class LoadSamoa(Base):
    __tablename__ = 'samoa'
    id = Column(Integer, primary_key=True)
    Round = Column(String)
    Demograph = Column(String)
    Demograph_Value = Column(String)
    Indicator = Column(String)
    Indicator_Value = Column(Float)
    Weighted_Count = Column(Float)

    def __repr__(self):
        return f"<LoadPacificMVAM(Round={self.Round}, Demograph_Value={self.Demograph_Value},\
            Indicator={self.Indicator}, Indicator_Value={self.Indicator_Value}>)"