import setuptools

setuptools.setup(
    name='lausanne-greening-scenarios',
    packages=setuptools.find_packages(exclude=['docs', 'tests*']),
    version='0.1.0',
    description=
    'Evaluation of the impact of a set of greening scenarios on air temperature in Lausanne',
    author='Martí Bosch',
    license='GPL-3.0',
)
