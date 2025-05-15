from setuptools import setup, find_packages

setup(
    name="address_matching",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        'pandas',
        'psutil',
        'tqdm',
        'rapidfuzz',
        'python-Levenshtein',
        'metaphone',
        'soundex',
        'usaddress',
        'psycopg2-binary',
        'SQLAlchemy',
        'fastapi',
        'uvicorn',
        'pyyaml',
        'pytest'
    ]
)
