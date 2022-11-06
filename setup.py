#!/usr/bin/env python
import os
import os.path

from setuptools import find_namespace_packages, setup

from src.cirrus.plugins.management import DESCRIPTION, NAME

HERE = os.path.abspath(os.path.dirname(__file__))
VERSION = os.environ.get("PLUGIN_VERSION", "0.0.0")

with open(os.path.join(HERE, "README.md"), encoding="utf-8") as f:
    README = f.read()

with open(os.path.join(HERE, "requirements.txt"), encoding="utf-8") as f:
    reqs = f.read().split("\n")

INSTALL_REQUIRES = [x.strip() for x in reqs if "git+" not in x]
DEPENDENCY_LINKS = [x.strip().replace("git+", "") for x in reqs if "git+" not in x]

setup(
    name=NAME,
    packages=find_namespace_packages("src"),
    package_dir={"": "src"},
    version=VERSION,
    description=DESCRIPTION,
    long_description=README,
    long_description_content_type="text/markdown",
    author="Jarrett Keifer (jkeifer), Element 84",
    url="https://github.com/cirrus-geo/cirrus-mgmt",
    install_requires=INSTALL_REQUIRES,
    dependency_links=DEPENDENCY_LINKS,
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    license="Apache-2.0",
    include_package_data=True,
    entry_points="""
        [cirrus.plugins]
        {NAME}=cirrus.plugins.management
        [cirrus.commands]
        manage=cirrus.plugins.management.commands.manage:manage
        payload=cirrus.plugins.management.commands.payload:payload
        deployments=cirrus.plugins.management.commands.deployments:deployments
    """,
)
