from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class WriteReview(Base):
    """ Write Review """

    __tablename__ = "write_review"

    id = Column(Integer, primary_key=True)
    Post_id = Column(String(250), nullable=False)
    Username = Column(String(250), nullable=False)
    Rate_no = Column(Integer, nullable=False)
    Review_description = Column(String(250), nullable=False)
    Time_posted = Column(String(100), nullable=False)
    date_created = Column(DateTime, nullable=False)

    def __init__(self, Post_id, Username, Rate_no, Review_description):
        """ Initializes a heart rate reading """
        self.Post_id = Post_id
        self.Username = Username
        self.Rate_no = Rate_no
        self.Review_description = Review_description
        self.Time_posted = datetime.datetime.now()  # Sets the date/time record is created
        self.date_created = datetime.datetime.now()

    def to_dict(self):
        """ Dictionary Representation of a heart rate reading """
        dict = {'id': self.id,
                'Post_id': self.Post_id,
                'Username': self.Username,
                'Rate_no': self.Rate_no,
                'Review_description': self.Review_description,
                'Time_posted': self.Time_posted,
                'date_created': self.date_created
                }

        return dict
