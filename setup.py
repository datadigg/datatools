import os
from setuptools import find_packages
from setuptools import setup


def parse_requirements(filename):
    with open(filename) as requirements_file:
        return requirements_file.readlines()


setup(
    # Application name:
    name="datatools",

    # Version number (initial):
    version="0.3.0",

    # Application author details:
    author="tutunannan",
    author_email="bd66_6@hotmail.com",

    # Packages
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),

    # Include additional files into the package
    include_package_data=True,

    # Details
    # url="http://pypi.abc.com/pypi/datatools_v030",

    #
    # license="LICENSE.txt",
    description="Simple ETL Data Tools",

    long_description=open("README.md").read(),

    # Dependent packages (distributions)
    install_requires=parse_requirements('requirements.txt'),
)
