import sys, getopt
from  os import path
import pandas as pd
from datetime import datetime

"""
Exit Status:
2: Reading arguments
3: Input file problems
4: Output file problems
"""

def main (argv):
    # Defining options in for command line arguments
    options = "hf:o:"
    long_options = ["file=", "output_file="]
    
    inputFile = ''
    outputFile = ''
    
    # Extracting arguments
    try:
        opts, args = getopt.getopt(argv, "hf:o:", ["file=", "output_file="])
    except getopt.GetoptError:
        print ("Error generated")
        sys.exit(2)

    for option, value in opts:
        if option == '-h':
            print ("Asked for help")
        elif option in ("-f", "--file"):
            inputFile = value
        elif option in ("-o", "--output_file"):
            outputFile = value
    
    # Calidate paths
    outputFile = validateFilePath(inputFile, outputFile)
    print ("initiating transformation...\nInput file: {}\nOutput File: {}".format(inputFile, outputFile))

    
    # Read CSV
    csvfile = pd.read_csv(inputFile)

    # Get a single row
    print (csvfile)


def validateFilePath (inputPath, output):
    """
    Function to validate the input and output file path.
        - The input file must exist, must be non-empty, and should have a .csv extension
        - The output file must have a .csv extension
    
    Parameters
    ----------
        - inputPath : str
            Input file path
        - output : str
            Output file path
    
    Returns
    -------
        - output : str
            An output file path which is the same if validated and a generic name if the original string was empty.
    """
    
    # Check if input file path was provided
    if inputPath == '':
        print ("An input file is necessary for the script to run. Use -f or --file option to define input file path.")
        sys.exit(3) 
    # Check if input file path is a csv file
    elif not inputPath.endswith(".csv"):
        print ("Only CSV files are allowed (.csv).")
        sys.exit(3)
    # Check if input file path exists
    elif not path.exists(inputPath):
        print ("The file you specified does not exist. Please recheck the path.")
        sys.exit(3)

    # If output file path was not given...
    if output == '':
        tempdate = datetime.now()
        tempnameSuffix = str(tempdate.day) + '-' + str(tempdate.month) + '-' + str(tempdate.year) + '_' + str(tempdate.hour) + '.' + str(tempdate.minute) + '.' + str(tempdate.second)
        output = "_ExportSureDoneEPID_" + tempnameSuffix
    else:
        # Check if a csv file path is defined for the output
        if not output.endswith(".csv"):
            print ("Only CSV files are allowed (.csv).")
            sys.exit(4)
    return output

# Running script from command line
if __name__ == "__main__":
    main(sys.argv[1:])