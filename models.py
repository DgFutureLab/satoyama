from database import Base, db_session
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Float
from sqlalchemy.exc import IntegrityError, DataError
from sqlalchemy.orm import relationship
import uuid
from collections import Iterable
import json
import config
from datetime import datetime

class ExpectedFieldException(Exception):
	def __init__(self, field):
		message = 'Expected keyword for field %s'%field
		super(ExpectedFieldException, self).__init__(message)




def create(model):						### 'create' is the name of the decorator
	@staticmethod
	def autocommit(*args, **kwargs): 		### Gets arguments for the object to create
		instance = model(*args, **kwargs) 	### Instance is created
		try:
			db_session.add(instance)			### ..added to the seesion
			db_session.commit()					### ..inserted to the database
			return instance 					### ..and returned to the caller
		except Exception, e:
			db_session.rollback()
			print e
		
	model.create = autocommit 			###	The model class (e.g. Node, Sensor, etc, is given a create method which calls the inner function) 		
	return model						### The decorated model class is returned and replaces the origin model class


@create
class Node(Base):
	
	__tablename__ = 'nodes'
	
	id = Column( Integer, primary_key = True )
	uuid = Column( String(32), unique = True)
	alias = Column( String(100) )
	sensors = relationship('Sensor', backref = 'node')

	def __init__(self, alias = None, sensors = []):
		assert isinstance(sensors, Iterable), 'sensors must be iterable'
		for sensor in sensors:
			assert isinstance(sensor, Sensor), 'Each item in sensors must be an instance of type Sensor'
			self.sensors.append(sensor)

		self.uuid = uuid.uuid4().hex
		self.alias = alias

	def json_detailed(self):
		return {'type': '<Node>', 'uuid': self.uuid, 'alias':self.alias, 'sensors': map(lambda s: s.json_summary(), self.sensors)}

	def json_summary(self):
		return {'type': '<Node>', 'uuid': self.uuid, 'alias':self.alias, 'number of sensors': len(self.sensors)}
	
	def __repr__(self):
		return json.dumps(self.json_summary())


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

	def json_detailed(self):
		return {'type': '<SensorType>', 'name': self.name, 'unit': self.unit, 'sensors':len(self.sensors), 'id': self.id}

	def json_summary(self):
		return {'type': '<SensorType>', 'name': self.name, 'id': self.id}

	def __repr__(self):
		return json.dumps(self.json_summary())


@create
class Sensor(Base):
	__tablename__ = 'sensors'
	
	id = Column( Integer, primary_key = True )
	uuid = Column( String(32), unique = True )
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
		self.uuid = uuid.uuid4().hex
		self.alias = alias
		
		for reading in readings:
			assert isinstance(reading, Reading), 'Each item in readings must be an instance of type Reading'
			self.readings.append(reading)

	def json_detailed(self):
		return {'type': '<Sensor>', 'uuid': self.uuid, 'sensor_type':self.sensortype.json_summary(), 'readings': map(lambda r: r.json_detailed(), self.readings)}

	def json_summary(self):
		return {'type': '<Sensor>', 'uuid': self.uuid, 'number of readings': len(self.readings)}

 
	def __repr__(self):
		return json.dumps(self.json_summary())

@create
class Reading(Base):
	__tablename__ = 'readings'

	id = Column( Integer, primary_key = True )
	timestamp = Column( DateTime() )
	value = Column( Float() )

	sensor_id = Column( Integer, ForeignKey('sensors.id') )


	def __init__(self, sensor, value = None, timestamp = None):
		# assert isinstance(sensor, Sensor), 'sensor must be an instance of type Sensor'
		self.sensor = sensor
		
		try:
			self.value = float(value)
		except DataError, e:
			value = None
			raise e

		if timestamp: 
			try:
				self.timestamp = datetime.strptime(timestamp, config.DATETIME_FORMAT)
			except ValueError, e:
				raise e
		else:
			timestamp = None
			

	def json_detailed(self):
		return {'type': '<Reading>', 'value': self.value, 'timestamp': self.timestamp, 'sensor': self.sensor}

	def json_summary(self):
		return {'type': '<Reading>', 'value': self.value, 'id': self.id}

	def __repr__(self):
		return json.dumps(self.json_summary())

		
def get_testobjects():

	n = Node.create(alias = 'testnode')
	st = SensorType.create(name = 'temperature', unit = 'C')
 	s = Sensor.create(sensortype = st, node = n)
 	r = Reading.create(sensor = s)
 	return n, st, s, r




