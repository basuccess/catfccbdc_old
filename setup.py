from setuptools import setup, find_packages

setup(
    name='catfccbdc',
    version='0.1.0',
    author='Tony Houweling',
    author_email='tony.houweling@gmail.com',
    description='A project to build broadband service geopackage files for US states and territories.',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'pandas',
        'geopandas',
        'tpdm',
    ],
    entry_points={
        'console_scripts': [
            'catfccbdc=main:main',
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
