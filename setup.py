import setuptools

# with open("README.md", "r") as fh:
#     long_description = fh.read()

print(setuptools.find_packages())

setuptools.setup(
    name="yumr",  # Replace with your own username
    version="0.0.1",
    author="Mr.Yum Data Engineering Team",
    author_email="aiml@mryum.com",
    description="ETL Demo",
    long_description="",
    long_description_content_type="text/markdown",
    url="n/a",
    packages=['yumr'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: pair_id_scope/A",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=['pyspark==3.2.0',
                      'pandas==1.3.4',
                      'pytest==6.2.4',
                      'jsonargparse==3.19.4']
)
