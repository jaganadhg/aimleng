
from sqlalchemy import (Column,String, Integer,BigInteger,
                        Float, PrimaryKeyConstraint, Date,
                        DateTime, TIME, DATE)
import sqlalchemy as sal
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (relationship, backref)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy import inspect
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql.expression import (func,
                                       case)

BaseModel = declarative_base()

class RetailDataUCI(BaseModel):
    """
    UCI retail data
    """
    
    __tablename__ = "retail_store"
    
    invoice = Column(String(20))
    stock_code = Column(String(20))
    description = Column(String(100))
    quantity = Column(BigInteger())
    invoice_date = Column(DateTime(timezone=False))
    price = Column(Float(precision=2))
    cust_id = Column(BigInteger())
    country = Column(String(100))
    hash_key = Column(String(150),
                      primary_key=True)