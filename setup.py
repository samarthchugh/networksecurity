'''
The setup.py file is an essential part of packaging and distributing Python projects. It is used by setuptools 
(or distutils in older Python versions) to define the configuration of your project, such as its metadata, dependencies, and more.
'''

from setuptools import setup, find_packages
from typing import List

def get_requirements() -> List[str]:
    '''
    This function will return list of requirements
    '''
    requirement_lst: List[str] =[]
    try:
        with open('requirements.txt') as f:
            # Read lines from the file
            lines = f.readlines()
            ## Process each line
            for line in lines:
                requirements=line.strip()
                # ignore empty lines and -e.
                if requirements and not requirements!='-e':
                    requirement_lst.append(requirements)
    except FileNotFoundError:
        print("Requirements file not found")
        
    return requirement_lst

print(get_requirements())