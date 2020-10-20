from setuptools import setup

requirements = [
    # package requirements go here
]

setup(
    name="tweeps",
    version="0.1.0",
    description="Short description",
    author="Curtis Hampton",
    author_email="CurtLHampton@gmail.com",
    url="https://github.com/CurtLH/tweeps",
    packages=["tweeps"],
    entry_points={"console_scripts": ["tweeps=tweeps.cli:cli"]},
    install_requires=requirements,
    keywords="tweeps",
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
    ],
)
