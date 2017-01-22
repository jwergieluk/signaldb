from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='signaldb',
    version='0.0.1',
    description='A market data system for financial time-series',
    long_description=readme,
    author='Julian Wergieluk',
    author_email='julian@wergieluk.com',
    url='https://www.wergieluk.com',
    license=license,
    install_requires=[],
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3'
    ],
)
