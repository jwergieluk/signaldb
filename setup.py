from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='signaldb',
    version='0.0.2',
    description='A market data system for financial time-series',
    long_description=readme,
    author='Julian Wergieluk',
    author_email='julian@wergieluk.com',
    url='http://www.wergieluk.com',
    license=license,
    install_requires=['appdirs', 'click', 'packaging', 'pymongo', 'pyparsing',
                      'python-dateutil', 'pytz', 'six', 'tonyg-rfc3339', 'finstruments'],
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3'
    ],
    entry_points={'console_scripts': ['sdb = signaldb.__main__:cli']}
)

