from database import Base, db_session
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Float
from sqlalchemy.exc import IntegrityError, DataError
from sqlalchemy.orm import relationship, object_mapper, class_mapper
from collections import Iterable
import json
import config
from datetime import datetime


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



# class Place(Base):
# 	pass


class SatoyamaBase():

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
class Node(Base, SatoyamaBase):
	
	__tablename__ = 'nodes'
	
	id = Column( Integer, primary_key = True )
	alias = Column( String(100) )
	sensors = relationship('Sensor', backref = 'node')
	longitude = Column( Float()) 
	latitude = Column( Float())

	def __init__(self, alias = None, sensors = [], longitude = None, latitude = None):
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
class SensorType(Base):
	__tablename__ = 'sensortypes'

	id = Column( Integer, primary_key = True)
	name = Column( String() )
	unit = Column( String() )
	sensors = relationship('Sensor', backref = 'sensortype')

	def __init__(self, name, unit):
		self.name = name
		self.unit = unit

	def __repr__(self):
		return str({'id' : self.id})


@create
class Sensor(Base, SatoyamaBase):
	__tablename__ = 'sensors'
	
	id = Column( Integer, primary_key = True )
	alias = Column( String() )

	readings = relationship('Reading', backref = 'sensor')
	node_id = Column( Integer, ForeignKey('nodes.id') )
	sensortype_id = Column( Integer, ForeignKey('sensortypes.id') )
	
	def __init__(self, sensortype, node, alias = None, readings = []):
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
class Reading(Base):
	__tablename__ = 'readings'

	id = Column( Integer, primary_key = True )
	timestamp = Column( DateTime() )
	value = Column( Float() )

	sensor_id = Column( Integer, ForeignKey('sensors.id') )


	def __init__(self, sensor, value = None, timestamp = None):
		self.sensor = sensor
		
		try:
			self.value = float(value)
		except DataError, e:
			value = None
			raise e

		if timestamp: 
			timestamp = get_longest_timestamp(timestamp)

		if timestamp:
			self.timestamp = timestamp
		else:
			raise ValueError('Provided timestamp matched none of the allowed datetime formats')
	
	def __repr__(self):
		return str({'id' : self.id})

	
		
def get_longest_timestamp(timestring):
	for format in config.DATETIME_FORMATS:
		timestamp = None
		try:
			timestamp = datetime.strptime(timestring, format)
			break
		except ValueError:
			pass
	return timestamp


def get_testobjects():

	n = Node.create(alias = 'testnode')
	st = SensorType.create(name = 'temperature', unit = 'C')
 	s = Sensor.create(sensortype = st, node = n)
 	r = Reading.create(sensor = s)
 	return n, st, s, r




