import uuid
from setuptools import setup, find_packages
import os
from pip.req import parse_requirements

# parse_requirements() returns generator of pip.req.InstallRequirement objects
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
reqs_file = os.path.join(BASE_DIR, 'requirements.txt')
install_reqs = parse_requirements(reqs_file, session=uuid.uuid1())

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]

setup(
    # Application name:
    name="datatools",

    # Version number (initial):
    version="0.1.0",

    # Application author details:
    author="Xu Cheng",
    author_email="cheng.xu@tendata.cn",

    # Packages
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="http://pypi.tendata.cn/pypi/datatools_v010",

    #
    # license="LICENSE.txt",
    description="Data Tools",

    long_description=open("README.md").read(),

    # Dependent packages (distributions)
    install_requires=reqs,
)
