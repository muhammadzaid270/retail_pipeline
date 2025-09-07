'''
NOTE: 
    To run this test correctly, run it from the project root:
        python tests/test_check_files.py 
    or to run all tests:
        python -m unittest discover -s tests -p "test*.py"
'''

import unittest
from pathlib import Path
import tempfile
from src.check_files import get_csv_files

class TestGetCSVFiles(unittest.TestCase):

    def test_with_csv_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            folder = Path(temp_dir)
            (folder / "file1.csv").touch()
            (folder / "file2.csv").touch()
            (folder / "file3.txt").touch() #non csv

            result = get_csv_files(folder)
            result_names = [f.name for f in result]

            self.assertIn("file1.csv", result_names)
            self.assertIn("file2.csv", result_names)
            self.assertNotIn("file3.txt", result_names)

    def test_no_csv_files(self):
        with tempfile.TemporaryDirectory() as temp_dir: 
            folder = Path(temp_dir)
            (folder / "file1.txt").touch()

            result = get_csv_files(folder)
            self.assertEqual(result, [])  # Should return empty list

if __name__ == "__main__":
    unittest.main()
