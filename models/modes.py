import os
import sys
import datetime as dt
import logging
import numpy as np

from sqlalchemy import (Column,String, Integer,BigInteger,
                        Float, PrimaryKeyConstraint, Date,
                        DateTime, TIME, DATE)
import sqlalchemy as sal
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (relationship, backref)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy import (inspect,
                        type_coerce)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql.expression import (func,
                                       case,
                                       cast)

from pandas.tseries.holiday import USFederalHolidayCalendar as calendar

logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s')

sys.path.append(os.path.join(os.path.dirname(__file__)))


BaseModel = declarative_base()


class RefModel(BaseModel):
    __abstract__ = True
    def get_data(self) -> dict:
        """
            Get all the attributes including hybrid properties
            Reference - https://stackoverflow.com/questions/1958219/
        """
        dict_ = {}
        for key in self.__mapper__.c.keys():
            if not key.startswith('_'):
                dict_[key] = getattr(self, key)

        for key, prop in inspect(self.__class__).all_orm_descriptors.items():
            if isinstance(prop, hybrid_property):
                dict_[key] = getattr(self, key)
        return dict_


class RetailData(RefModel):
    """
        Model for the UCI retail Data in PostgrsSQL
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
    
    
    @hybrid_property
    def total_cost(self) -> float:
        """
            Total cost = quantity * price
        """
        return self.quantity * self.price
     
    @total_cost.expression
    def total_cost(cls) -> float:
        return type_coerce(cls.quantity * cls.price)
    
    #total_cost.__setattr__("property", "None")
    
    @hybrid_property
    def cancelled_order(self) -> bool:
        """
            A cancelled order is prefixed with C
        """
        is_cancelled = False
        if "C" in self.invoice:
            is_cancelled = True
        
        return is_cancelled
    
    @cancelled_order.expression
    def cancelled_order(cls):
        return cls.invoice.like('C%')


class ElectricityData(RefModel):
    """
        Model for the electricity data
    """
    
    __tablename__ = "elec_usage"
    
    usage_type = Column(String(length=20))
    usage_date = Column(DATE())
    start_time = Column(TIME(timezone=False))
    end_time = Column(TIME(timezone=False))
    usage_in_kwh = Column(Float(precision=5))
    units = Column(String(length=5))
    cost_in_dollor = Column(String(length=5))
    notes = Column(String(length=100))
    cust_id = Column(String(length=10))
    rec_id = Column(Integer(),
                    primary_key=True)
    
    @hybrid_property
    def date_time(self):
        date_times = dt.datetime.combine(self.usage_date,
                                         self.end_time)
        return date_times
    
    @date_time.expression
    def date_time(cls):
        return cls.usage_date + cls.end_time
    
    @hybrid_property
    def day_type(self):
        is_week_day = 1
        week_day = self.usage_date.isocalendar()[2]
        print(week_day)
        
        if week_day > 5:
              is_week_day = 0
        
        return is_week_day
    
    @day_type.expression
    def day_type(cls):
        from sqlalchemy import true
        
        return case([(sal.extract('isodow',cls.usage_date).__gt__(5), 1),],
                    else_ = 0)
    
    @hybrid_property
    def is_holiday(self):
        is_hday = 0
        cal = calendar()
        holidays = cal.holidays(start=dt.date(2015,1,1),
                                     end=dt.date(2020,12,31))
        if np.datetime64(self.usage_date) in holidays:
            is_hday = 1
        
        return is_hday
    
    @is_holiday.expression
    def is_holiday(cls):
        """
            Reference - https://stackoverflow.com/questions/64276059
        """
        is_hday = 0
        cal = calendar()
        holidays = cal.holidays(start=dt.date(2015,1,1),
                                     end=dt.date(2020,12,31))
        
        is_hday = cls.usage_date.in_(holidays)
        return is_hday

    def __init__(self, **kwargs) -> None:
        super(ElectricityData, self).__init__(**kwargs)