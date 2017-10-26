from setuptools import setup

requirements = [
    # package requirements go here
]

setup(
    name='tweeps',
    version='0.1.0',
    description="The tweeps application provides a simple way to connect to the the Twitter Streaming API. After you submit your keywords and hashtags to tweeps, the application will maintain a constant connection to the API, and store new tweets in a PostgreSQL database as they come into the system. The application also includes an ETL process that will regularily monitor the source database and routinely extract, transform, and load data from the source database into a data warehouse.",
    author="Curtis Hampton",
    author_email='CurtLHampton@gmail.com',
    url='https://github.com/CurtLH/tweeps',
    packages=['tweeps'],
    entry_points={
        'console_scripts': [
            'tweeps=tweeps.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='tweeps',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
    ]
)
