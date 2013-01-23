#!/usr/bin/env python
from setuptools import setup, find_packages
setup(
    name = "linear-flow",
    version = '0.9.3',
    packages = ['flow'],
    package_dir = {'':'src'},   # tell distutils packages are under src
    zip_safe = True,
    author = "Darren Hardy",
    author_email = "hardy@nceas.ucsb.edu",
    description = "A Swiss-army knife for scientific workflows using linear workflows",
    url = "http://www.nceas.ucsb.edu/",
    long_description = '''Flow is a *Format*-neutral, *Language*-agnostic, *Open*-source *Workflow* tool
-- a veritible Swiss-army knife for scientific workflow integration,
automation, execution, and reporting. Flow cares not about which data formats
or software tools you use. Flow cares only about running workflows with
minimal integration effort. Flow uses a database of workflow *styles* and a
simple linear process model to automate execution for a myrid of different
scientific workflow styles. For example, a 'simple' style uses the following
process model: Setup, Import, Ingest, Model, Digest, Export, and Finish.
''',
    entry_points='''
[console_scripts]
flow = flow.shell:main
'''

)
