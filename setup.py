
from setuptools import setup, find_packages

setup(
    name="Remote_DWH_Accelarator",  # Name of the package
    version="0.1",  # Initial version
    packages=find_packages(),  # Automatically find packages inside your project
    description="Utilities for CK DWH Accelerator",  # A short description of your project
    author="Surya",  # Your name
    
    url="https://github.com/SWE-Surya/Remote_DWH_Accelarator",  # Repository URL
    classifiers=[
        "Programming Language :: Python :: 3",  # Compatible Python version
        "License :: OSI Approved :: MIT License",  # Your project license
        "Operating System :: OS Independent",  # Compatibility
    ],
    python_requires='>=3.6',  # Minimum Python version required

)
