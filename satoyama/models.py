from database import Base, db_session
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Float
from sqlalchemy.exc import IntegrityError, DataError
from sqlalchemy.orm import relationship, object_mapper, class_mapper
from collections import Iterable
from helpers import DatetimeHelper

def create(model):						### 'create' is the name of the decorator
	@staticmethod
	def autocommit(*args, **kwargs): 		### Gets arguments for the object to create
		try:
			instance = model(*args, **kwargs) 	### Instance is created		
		except Exception, e:
			db_session.rollback()
			raise e

		if instance:
			try:
				db_session.add(instance)			### ..added to the session
				db_session.commit()					### ..inserted to the database
				return instance 					### ..and returned to the caller
			except Exception, e:
				db_session.rollback()
				print e

	model.create = autocommit 			###	The model class (e.g. Node, Sensor, etc, is given a create method which calls the inner function) 		
	return model						### The decorated model class is returned and replaces the origin model class

class SatoyamaBase():

	def __init__(self):
		self.messages = list()

	def json(self, verbose = False):
		jsondict = {}
		for prop in object_mapper(self).iterate_properties: 
			jsondict.update({prop.key: getattr(self, prop.key)})
		return jsondict

	@classmethod
	def settables(cls):
		"""
		Returns a list of which fields in the inhereted model can be set when instantiating the class.
		"""
		return filter(lambda x: x != 'id', [p.key for p in class_mapper(cls).iterate_properties])

@create
class Node(SatoyamaBase, Base):
	
	__tablename__ = 'nodes'
	
	id = Column( Integer, primary_key = True )
	alias = Column( String(100) )
	sensors = relationship('Sensor', backref = 'node')
	longitude = Column( Float()) 
	latitude = Column( Float())

	def __init__(self, alias = None, sensors = [], longitude = None, latitude = None):
		super(Node, self).__init__()
		assert isinstance(sensors, Iterable), 'sensors must be iterable'
		for sensor in sensors:
			assert isinstance(sensor, Sensor), 'Each item in sensors must be an instance of type Sensor'
			self.sensors.append(sensor)

		self.longitude = longitude
		self.latitude = latitude
		self.alias = alias

	def __repr__(self):
		return str({'id' : self.id})

@create
class SensorType(SatoyamaBase, Base):
	__tablename__ = 'sensortypes'

	id = Column( Integer, primary_key = True)
	name = Column( String() )
	unit = Column( String() )
	sensors = relationship('Sensor', backref = 'sensortype')

	def __init__(self, name, unit):
		super(SensorType, self).__init__()
		self.name = name
		self.unit = unit

	def __repr__(self):
		return str({'id' : self.id})

@create
class Sensor(SatoyamaBase, Base):
	__tablename__ = 'sensors'
	
	id = Column( Integer, primary_key = True )
	alias = Column( String() )
	readings = relationship('Reading', backref = 'sensor')
	node_id = Column( Integer, ForeignKey('nodes.id') )
	sensortype_id = Column( Integer, ForeignKey('sensortypes.id') )
	
	def __init__(self, sensortype, node, alias = None, readings = []):
		super(Sensor, self).__init__()
		assert isinstance(sensortype, SensorType), 'sensortype must be an instance of type SensorType'
		assert isinstance(node, Node), 'node must be an instance of type Node'
		assert isinstance(readings, Iterable), 'readings must iterable'
		
		self.sensortype = sensortype
		self.node = node
		self.alias = alias
		
		for reading in readings:
			assert isinstance(reading, Reading), 'Each item in readings must be an instance of type Reading'
			self.readings.append(reading)

	def __repr__(self):
		return str({'id' : self.id})

@create
class Reading(SatoyamaBase, DatetimeHelper, Base):
	__tablename__ = 'readings'

	id = Column( Integer, primary_key = True )
	timestamp = Column( DateTime() )
	value = Column( Float() )
	sensor_id = Column( Integer, ForeignKey('sensors.id') )

	def __init__(self, sensor, value = None, timestamp = None):
		super(Reading, self).__init__()
		self.sensor = sensor
		
		if value:
			try:
				self.value = float(value)
			except DataError, e:
				value = None
				raise e

		self.timestamp = self.convert_timestamp(timestamp)
	
	def __repr__(self):
		return str({'id' : self.id})

