from setuptools import setup, find_packages

import barbante


setup(name='barbante',
      version=barbante.__version__,
      description='Recommendation Algorithms',
      packages=find_packages(),
      package_dir={'barbante': 'barbante'},
      package_data={'barbante': ['config/*.yml', 'VERSION']},
      setup_requires=['setuptools==3.6',
                      'pip==1.5.6',
                      'nose==1.3.3',
                      'nosexcover==1.0.10'],
      install_requires=['mmh3==2.3',
                        'python3-memcached==1.51',
                        'nltk==3.0.0b1',
                        'numpy==1.9.1',
                        'pymongo==2.7.2',
                        'pyyaml==3.11',
                        'pytz==2014.9',
                        'tornado==4.0'],
      test_suite='nose.collector')
