"""
File created mainly for testing and debug purposes.
"""

import os
from os import path
import sys
import pandas as pd

def sortCSV(csvPath):
    """
    Function that will take a csv path, open it, and sort it based on GUIDs.
    This is to make sure that all the same GUIDs are placed together. Program's
    memory usage will become very large if every GUID is being managed at the same
    time in a case where there are millions of rows.

    Parameters
    ----------
        - csvPath : str
            A path to the source csv file
    
    Returns
    -------
        - csv : Data Frame (pandas)
            A DataFrame object that contains the sorted version of the source csv
    """
    csv = pd.read_csv(csvPath)
    csv = csv.sort_values(['guid'])
    return csv
# sortCSV('Samples\\source_large.csv')

def concat(to, file):
    # Open files
    # outputfile = open(file, 'w')
    

    with open(to, 'r') as originalfile:
        lines = originalfile.readlines()
    with open(to, 'r') as originalfile:
        linesToCopy = originalfile.readlines()[1:]

    finalLines = lines + linesToCopy
    print (len(lines))
    print (len(linesToCopy))
    print (len(finalLines))

    # Close files
    
    #outputfile.close()



sourcepath = path.join(os.getcwd(), 'Samples\\source_large.csv')
outpath = path.join(os.getcwd(), 'Samples\\source_large_double.csv')
concat(sourcepath, outpath)
