import sys
import pandas as pd

# sys.argv holds all the arguments passed after the keyword "python" in shell
# example: python test.py 10 20 30
# output: ['test.py', '10', '20', '30']
args = [i for i in sys.argv]
print(f"pandas version: {pd.__version__}")
print(args)