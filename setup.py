import os
from setuptools import setup, find_packages

# Read requirements from requirements.txt
with open("requirements.txt") as f:
    requirements = f.read().splitlines()

# Read long description from README.md
with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="kafka-shell",
    version="0.1.0",
    description="A Python CLI tool that simplifies Kafka interaction from your terminal",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Momar T. Cisse",
    author_email="momar@thepinkchannel.com",
    python_requires=">=3.12.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=requirements,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="kafka, cli, shell, messaging, confluent",
    project_urls={
        "Bug Tracker": "https://github.com/blvckpvblo/kafka-shell/issues",
        "Documentation": "https://github.com/blvckpvblo/kafka-shell",
        "Source Code": "https://github.com/blvckpvblo/kafka-shell",
    },
    include_package_data=True,
    zip_safe=False,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "flake8>=6.0.0",
            "black>=23.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "kafka-shell=main:main",
        ],
    },
)
