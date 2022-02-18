from pathlib import Path

from setuptools import find_packages, setup

this_directory = Path(__file__).parent
install_requires = (this_directory / "requirements.txt").read_text().splitlines()

# get version
with open("pvconsumer/__init__.py") as f:
    for line in f:
        if line.startswith("__version__"):
            _, _, version = line.replace("'", "").split()
            version = version.replace('"', "")

setup(
    name="pvconsumer",
    version=version,
    packages=find_packages(),
    install_requires=install_requires,
)
