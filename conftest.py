import sys
import os

# Make the project root and subdirectories importable
root_dir = os.path.dirname(__file__)
sys.path.insert(0, root_dir)
sys.path.insert(0, os.path.join(root_dir, 'aws', 'modules'))
sys.path.insert(0, os.path.join(root_dir, 'aws', 'scripts', 'lambda_scripts'))
sys.path.insert(0, os.path.join(root_dir, 'aws', 'scripts', 'glue_scripts'))
