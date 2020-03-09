# -*- coding: utf-8 -*-
"""SQL Transformation

@author: Hassan Ahmed
@contact: ahmed.hassan.112.ha@gmail.com
@owner: Patrick Mahoney
@version: 0.1.5

This module is created to rearrange a CSV sheet expoerted from a SQL server.
The CSV has three columns:
    # GUID
    # EPID
    # note
The object of this script is to collectively arrange all EPIDs and their 
corresponding notes (if they exist) that have the same GUID. The file ouputs 
another CSV file which contains three columns:
    # action
        A constant column which will contain 'edit' string
    # guid
        GUID extracted from source
    # ebayepid
        A combination of EPIDs and notes that were logged against the same
        guid. (The same guid which is present in the second column)

The pattern to concatenate the epids and notes is:
    ebayepid (text) = Concatenation of 
    [Import File].[epid] If Exists ::[Import File].[note]
    *
    [Import File].[epid] If Exists ::[Import File].[note]
    *
    And so on unless the GUID changes or a number of defined
    iterations have been passed. These iterations are 1000 
    by default and can be changed through the command line.

Usage:
    From all the arguments available, the input file path is necessary and the
    script will not work if it is not provided.

    $ python rearrange.py -f [source.csv]
    $ python rearrange.py -file [source.csv]

    $ python rearrange.py -f [source.csv] -o [output.csv]
    $ python rearrange.py -file [source.csv] --output_file [output.csv]

    $ python rearrange.py -f [source.csv] -o [output.csv] -i 1000
    $ python rearrange.py -file [source.csv] --output_file [output.csv] --max_iterations 1000

Todo:
    * None right now

Exit Statusses:
    - 2 -- Reading arguments
    - 3 -- Input file problems
    - 4 -- Output file problems
    - 5 -- -h argument used
"""

import sys, getopt
from  os import path
import pandas as pd
from datetime import datetime
import time
from time import sleep
import tracemalloc

# Debug only
from  os import system

## Remove all debug variables
current_milli_time = lambda: int(round(time.time() * 1000)) # For time measurement

# Help message
HELP_MESSAGE = """
Usage:
    From all the arguments available, the input file path is necessary and the
    script will not work if it is not provided.
    
    $ python rearrange.py [-f <filepath>] [options]

Parameters/Options:
    -h                      : usage help and examples
    -f  | --file            : define input CSV path 
    -o  | --output_file     : define output CSV path
    -i  | --max_iterations  : define max iterations allowed on a single GUID
    -v  | --verbose         : show program execution details (may increase execution time)

Example:
    $ python rearrange.py -f [source.csv]
    $ python rearrange.py -file [source.csv]

    $ python rearrange.py -f [source.csv] -o [output.csv]
    $ python rearrange.py -file [source.csv] --output_file [output.csv]

    $ python rearrange.py -f [source.csv] -o [output.csv] -i 1000
    $ python rearrange.py -file [source.csv] --output_file [output.csv] --max_iterations 1000
"""

def main (argv):
    # Parse arguments
    inputFilePath, outputFilePath, maxItersPerGUID, VERBOSE = parseArgs(argv)
    print ("""Initiating transformation...
Input File              : {}
Output File             : {}
Max Iterations per GUID : {}
Verbose                 : {}""".format(inputFilePath, outputFilePath, maxItersPerGUID, VERBOSE))

    # Read CSV and get it sorted
    csvfile = readAndSortCSV(inputFilePath)

    # Memory usage analysis start
    # tracemalloc.start()

    # Loop through file
    outputcsv, finalTime = mainLoop(csvfile, maxItersPerGUID, VERBOSE)

    # Printing output to console and file
    outputcsv.to_csv(outputFilePath, index=False);
    print ("\nPrinting output...")
    
    if VERBOSE: print (outputcsv, end='\n\n')
    print ("Output CSV has been written to: {}".format(outputFilePath))
    if VERBOSE: print ("\nTime Taken by main loop: {} milliseconds".format(finalTime))

    # Get memory usage peak
    # current, peak = tracemalloc.get_traced_memory()
    # print(f"\nCurrent memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")
    # tracemalloc.stop()

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

def readAndSortCSV(csvPath):
    """
    Function that will take a csv path, open it, and sort it based on GUIDs.
    This is to make sure that all the same GUIDs are placed together. Program's
    memory usage will become very large if every GUID is being managed at the same
    time in a case where there are millions of rows.
    
    The function will also sort the EPIDs with the same GUID

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
    csv = csv.sort_values(['guid', 'Fitment EPID'])
    return csv

def parseArgs (argv):
    """
    Function that parses the arguments sent from the command line, validates
    the variables if needed, and returns it to the caller.

    Parameters
    ----------
        -argv : str
            Arguments sent through the command line
    
    Returns
    -------
        - outputFilePath : str
            File path of the source CSV file after validations
        - inputFilePath : str
            File path of the output CSV file after validations
        - maxItersPerGUID : int
            Max iterations allowed per GUID after validations
    """
    # Defining options in for command line arguments
    options = "hf:o:i:v"
    long_options = ["file=", "output_file=", "max_iterations=", "verbose"]
    
    # Arguments
    inputFilePath = ''
    outputFilePath = ''
    maxItersPerGUID = 1000
    VERBOSE = False
    
    # Extracting arguments
    try:
        opts, args = getopt.getopt(argv, options, long_options)
    except getopt.GetoptError:
        print ("Error in arguments!")
        print (HELP_MESSAGE)
        sys.exit(2)

    for option, value in opts:
        if option == '-h':
            print (HELP_MESSAGE)
            sys.exit(5)
        elif option in ("-v", "--verbose"):
            VERBOSE = True
        elif option in ("-f", "--file"):
            inputFilePath = value
        elif option in ("-o", "--output_file"):
            outputFilePath = value
        elif option in ("-i", "--max_iterations"):
            maxItersPerGUID = int(value)
            # Make sure that the value is in positive and non-zero
            if maxItersPerGUID < 1:
                maxItersPerGUID = 1000

    # Validate paths
    outputFilePath = validateFilePath(inputFilePath, outputFilePath)
    return inputFilePath, outputFilePath, maxItersPerGUID, VERBOSE

def mainLoop(csvfile, maxItersPerGUID, VERBOSE):
    """
    The main loop that will iterate through every row and manipulate data
    according to the arguments provided in the parameters.

    Parameters
    ----------
        - csvfile : pd.dataframe
            The dataframe that contains the contents of the input CSV file
        - maxItersPerGUID : int
            Maximum number of iteration allowed per GUID
        - VERBOSE : bool
            Whether to print looping details or not
    
    Returns
    -------
        - outputcsv : pd.dataframe
            Dataframe that containst the final contents to be saved in the output CSV
        - finalTime : long
            Time taken by the whole loop
    """
    # Create empty dataframe
    outputcsv = pd.DataFrame(columns=['action','guid','ebayepid'])

    # Read number of lines
    numrows = csvfile.shape[0]
    
    # Looping through all rows
    # Functional variables
    RUNNING_GUID = ''
    RUNNING_EBAYEPID = ''
    foundOnce = False
    currentGUIDIters = 0
    
    # Time measurement variables
    forStartTime = current_milli_time()
    
    # Progress publishing variables
    stepDiv = int(numrows / 100)
    steps = 0
    progress = 0
    progressPrefix = "Completed: {}%\t\t|\tIterations of current GUID: {}      "

    print ("\nStarting main loop...")
    for i in range (0, numrows):
        if VERBOSE:
            steps = steps + 1
            if (steps >= stepDiv):
                steps = 0
                progress = progress + 1
            
            if progress < 100:
                print (progressPrefix.format(progress, currentGUIDIters), end='\r')
            elif progress == 100 and i == numrows-1:
                print (progressPrefix.format(progress, currentGUIDIters))

        # Read one row
        row = csvfile.iloc[i]
        
        # Extract columns from row
        currentGUID = str(row[0])
        currentEPID = str(row[1])
        currentNote = str(row[2])

        # if currentNote is NaN, convert it to NoneType
        if currentNote == 'nan':
            currentNote = None

        # If the saved GUID is different than this row's GUID, then a new GUID has occured
        if not RUNNING_GUID == currentGUID:
            if foundOnce:
                # Add current guid and ebayepid to the output csv
                if len(RUNNING_EBAYEPID) > 0:
                    RUNNING_EBAYEPID = RUNNING_EBAYEPID[:-1]
                
                tempdf = pd.DataFrame([['edit', RUNNING_GUID, RUNNING_EBAYEPID]], columns=['action','guid','ebayepid'])
                outputcsv = outputcsv.append(tempdf, ignore_index=True)

            RUNNING_GUID = currentGUID
            RUNNING_EBAYEPID = ''
            currentGUIDIters = 0

            if not foundOnce:
                foundOnce = True

        # Check if iterations on current GUID have crossed the limit
        if currentGUIDIters >= maxItersPerGUID:
            continue
        
        # Concatenate current row's epid and note (if exists) to the RUNNING ebayepid
        toconcat = currentEPID
        if currentNote:
            toconcat += "::{}*".format(currentNote)
        else:
            toconcat += "*"
        RUNNING_EBAYEPID += toconcat
        currentGUIDIters = currentGUIDIters + 1

        # If the last row has arrived, add current guid and ebayepid to the output csv
        if i == numrows-1:
            if foundOnce:
                # Add current guid and ebayepid to the output csv
                if len(RUNNING_EBAYEPID) > 0:
                    RUNNING_EBAYEPID = RUNNING_EBAYEPID[:-1]
                
                tempdf = pd.DataFrame([['edit', RUNNING_GUID, RUNNING_EBAYEPID]], columns=['action','guid','ebayepid'])
                outputcsv = outputcsv.append(tempdf, ignore_index=True)
        # Debug message:
        """
        system('cls')
        print ("Completed:      {}%".format(progress))
        print ("Saved GUID:     " + RUNNING_GUID)
        print ("Saved EBAYEPID: " + RUNNING_EBAYEPID)
        print ("Current GUID :  " + currentGUID)
        print ("Current EPID :  " + currentEPID)
        print ("Current Note :  " + str(currentNote))
        print ("------------------------\n\n")
        """
    
    # Calculate time taken
    finalTime = current_milli_time() - forStartTime
    print ("Main loop Complete.")
    return outputcsv, finalTime

# Running script from command line
if __name__ == "__main__":
    main(sys.argv[1:])