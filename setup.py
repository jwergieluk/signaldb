from setuptools import setup, find_packages
import subprocess


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

with open('requirements.txt') as f:
    requirements = [p.strip().split('=')[0] for p in f.readlines() if p[0] != '-']


def get_development_version():
    git_output = subprocess.run(['git', 'rev-list', '--count', 'master'], stdout=subprocess.PIPE)
    return '0.0.%s' % git_output.stdout.decode('utf-8').strip()


setup(
    name='signaldb',
    version=get_development_version(),
    description='A market data system for financial time-series',
    long_description=readme,
    author='Julian Wergieluk',
    author_email='julian@wergieluk.com',
    url='http://www.wergieluk.com',
    license=license,
    install_requires=requirements,
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3'
    ],
    entry_points={'console_scripts': ['sdb = signaldb.__main__:cli']}
)

