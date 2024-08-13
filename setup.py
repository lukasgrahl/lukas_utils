from setuptools import setup 

setup( 
	name='lukas_utils', 
	version='0.11', 
	description='Common util functions for economics', 
	author='Lukas A. Grahl', 
	author_email='lukas.grahl@amkat.de', 
	packages=['lukas_utils'], 
	install_requires=[ 
		'numpy', 
		'pandas',
		'scipy',
		'matplotlib',
		'numba',
	], 
) 
