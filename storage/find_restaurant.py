from sqlalchemy import Column, Integer, String, DateTime, BLOB, Boolean
from base import Base
import datetime


class FindingRestaurant(Base):
    """ Blood Pressure """

    __tablename__ = "find_restaurant"

    id = Column(Integer, primary_key=True)
    Restaurant_id = Column(String(250), nullable=False)
    Location = Column(String(250), nullable=False)
    Restaurant_type = Column(String(100), nullable=False)
    Delivery_option = Column(String(100), nullable=False)
    Open_on_weekends = Column(String(100), nullable=False)
    date_created = Column(DateTime, nullable=False)

    def __init__(self, Restaurant_id, Location, Restaurant_type, Delivery_option, Open_on_weekends):
        """ Initializes a blood pressure reading """
        self.Restaurant_id = Restaurant_id
        self.Location = Location
        self.Restaurant_type = Restaurant_type
        self.Delivery_option = Delivery_option
        self.Open_on_weekends = Open_on_weekends
        self.date_created = datetime.datetime.now()  # Sets the date/time record is created

    def to_dict(self):
        """ Dictionary Representation of finding a restaurant """
        dict = {'id': self.id,
                'Restaurant_id': self.Restaurant_id,
                'Location': self.Location,
                'Restaurant_type': self.Restaurant_type,
                'Delivery_option': self.Delivery_option,
                'Open_on_weekends': self.Open_on_weekends,
                'date_created': self.date_created
                }

        return dict
