from setuptools import setup, find_packages
import os
about = {}
current = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(current, "kafka_plugin", "__version__.py"), "r", encoding="utf-8") as f:
    exec(f.read(), about)

with open("Readme.md", "r", encoding="utf-8") as f:
    readme = f.read()

setup(
    name=about["__title__"],
    version=about["__version__"],
    description=about["__description__"],
    long_description=readme,
    long_description_content_type="text/markdown",
    author=about["__author__"],
    author_email=about["__author_email__"],
    packages=['kafka_plugin'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)