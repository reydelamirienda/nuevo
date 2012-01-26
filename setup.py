from setuptools import setup, find_packages

requires = [
    "requests"
]

setup(name='nuevo',
    version='0.1-dev',
    description='A Python persistency framework and object mapping library'
                'designed for graph databases, like Neo4j.',
    author='Alberto Gonzalez',
    
    package_dir = {'': 'lib'},
    packages = find_packages('lib'),
    
    install_requires=requires,
    
    entry_points = {
        'nuevo.engines': [
            'neo4j=nuevo.drivers.neo4j.engine:load'
        ]
    }
)