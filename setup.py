import os
from setuptools import setup, find_packages

# Read requirements from requirements.txt
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

# Read long description from README.md
with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="kafka-message-producer",
    version="0.1.0",
    description="A Kafka message producer with comprehensive error handling and logging",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
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
    keywords="kafka, producer, messaging, confluent",
    project_urls={
        "Bug Tracker": "https://github.com/yourusername/kafka-message-producer/issues",
        "Documentation": "https://github.com/yourusername/kafka-message-producer",
        "Source Code": "https://github.com/yourusername/kafka-message-producer",
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
            "kafka-producer=producer.kafka_producer:main",
        ],
    },
)

