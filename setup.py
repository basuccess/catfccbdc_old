from setuptools import setup, find_packages

setup(
    name='catfccbdc',
    version='1.0.0',
    description='A project to build broadband service geopackage files for US states and territories.',
    author='Your Name',
    author_email='your.email@example.com',
    url='https://github.com/yourusername/catfccbdc',
    packages=find_packages(),
    install_requires=[
        'certifi==2025.1.31',
        'geopandas==1.0.1',
        'numpy==2.2.3',
        'pandas==2.2.3',
        'psutil==7.0.0',
        'pyproj==3.7.0',
        'python-dateutil==2.9.0.post0',
        'pytz==2025.1',
        'shapely==2.0.7',
        'tqdm==4.67.1',
        'tzdata==2025.1'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)