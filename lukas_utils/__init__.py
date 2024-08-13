import os

config = {
	'PROJECT_ROOT': os.getcwd(),
	'DATA_DIR': os.path.join(os.getcwd(), "data"),
	'GRAPHS_DIR': os.path.join(os.getcwd(), "graphs"),
}

from matplotlib import *
from statsmodels import *