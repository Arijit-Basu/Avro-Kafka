__author__ = 'Kunal'

#from distutils.core import setup
from setuptools import setup
setup(name='Python Avro',
      version='1.0',
      description='Convert JSON to Avro',
      author='Kunal Gupta',
      author_email='kunal.gupta@cube26.com',
      packages=['json_avro'],
      py_modules=['json_avro.run', 'json_avro.create_avro_schema'],
      package_data={'json_avro': ['json_value.json']},
      )
