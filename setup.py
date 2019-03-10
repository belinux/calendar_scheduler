import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="calendar_scheduler",
    version="0.0.2",
    author="Partha Saradhi Konda",
    author_email="parthasaradhi1992@gmail.com",
    description="Scheduler program",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/parthakonda/calendar_scheduler",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "croniter==0.3.27",
        "pytz==2018.9",
        "coverage==4.5.1"
    ],
)
