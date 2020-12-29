import pathlib
import pkg_resources
from setuptools import setup


full_version = "0.0.1"

with open("README.md", "r") as fh:
    long_description = fh.read()

with pathlib.Path('requirements.txt').open() as requirements_txt:
    install_requires = [
        str(requirement) for requirement in pkg_resources.parse_requirements(requirements_txt)
    ]

setup(
    name="steel_eye_proj",
    version=full_version,
    author="Seenu",
    author_email="srinivasulur55.s@gmail.com",
    description="Steel Eye Project",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/seenureddy/steel_eye_proj",
    include_package_data=True,
    packages=["steel_eye_dir"],
    install_requires=install_requires,
    entry_points={}
)
