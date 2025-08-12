import os
import unittest
import pandas as pd

from lukas_utils import save_pkl, load_pkl
from lukas_utils.helpers import load_df, save_df

path = os.path.join(os.getcwd(), 'df_test')

class TestLoadDF(unittest.TestCase):

	def test_load_file_not_found(self):
		with self.assertRaises(FileNotFoundError):
			load_df("does_not_exist.feather", path)
		pass

	def test_load_file_type(self):
		with self.assertRaises(KeyError):
			load_df("test.abc", path)
		pass

	def test_file_load(self):
		for file in ['test.feather', 'test.xlsx', 'test.csv']:
			self.assertIsInstance(load_df(file, 'df_test'), pd.DataFrame)

	def test_save_file_not_found(self):
		with self.assertRaises(FileNotFoundError):
			save_df(pd.DataFrame(range(10)), file_name='try_save.feather', file_path='does_not_exist')

	def test_save_file_type(self):
		with self.assertRaises(KeyError):
			save_df(pd.DataFrame(range(10)), file_name='try_save.abc')

class TestPKL(unittest.TestCase):

	def test_load_not_found(self):
		with self.assertRaises(FileNotFoundError):
			load_pkl('does_not_exist.pkl', file_path=path)

	def test_load(self):
		self.assertIsInstance(load_pkl('test.pkl', file_path=path), dict)

	def test_save_not_found(self):
		with self.assertRaises(FileNotFoundError):
			save_pkl(dict(), 'should_not_be_saved.pkl', file_path=os.path.join(os.getcwd(), "does_not_exist"))

if __name__ == "__main__":

	unittest.main()