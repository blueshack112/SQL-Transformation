import sys, getopt
from  os import path
import pandas as pd
from datetime import datetime
import time



# Debug only
from  os import system

"""
Exit Status:
2: Reading arguments
3: Input file problems
4: Output file problems
"""
## Remove all debug variables
current_milli_time = lambda: int(round(time.time() * 1000)) # For time measurement

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
    # TODO: uncomment this
    print ("Initiating transformation...\nInput file: {}\nOutput File: {}".format(inputFile, outputFile))
    
    # Create empty dataframe
    outputcsv = pd.DataFrame(columns=['action','guid','ebayepid'])

    # Read CSV and get it sorted
    csvfile = readAndSortCSV(inputFile)
    numrows = csvfile.shape[0]

    # Looping through all rows
    # TODO: add new looping options based on client's needs
    # TODO: Modularize as much as possible from here
    RUNNING_GUID = ''
    RUNNING_EBAYEPID = ''
    foundOnce = False
    forStartTime = current_milli_time()
    stepDiv = int(numrows / 100)
    steps = 0
    progress = 0
    progressPrefix = "Completed: {}%"
    print ("\nStarting main loop...")
    print (progressPrefix.format(progress), end='\r')    
    for i in range (0, numrows):
        steps = steps + 1
        if (steps >= stepDiv):
            steps = 0
            progress = progress + 1
            if progress == 100:
                print (progressPrefix.format(progress))
            else:
                print (progressPrefix.format(progress), end='\r')
        
        # Read one row
        row = csvfile.iloc[[i]]
        
        # Extract columns from row
        currentGUID = str(row.iloc[0][0])
        currentEPID = str(row.iloc[0][1])
        currentNote = str(row.iloc[0][2])

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

            if not foundOnce:
                foundOnce = True
        
        """
        Pattern:
        ebayepid (text) = Concatenation of 
        [Import File].[epid] If Exists ::[Import File].[note]
        *
        [Import File].[epid] If Exists ::[Import File].[note]
        """
        # Concatenate current row's epid and note (if exists) to the RUNNING ebayepid
        toconcat = currentEPID
        if currentNote:
            toconcat += "::{}*".format(currentNote)
        else:
            toconcat += "*"
        RUNNING_EBAYEPID += toconcat

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
        print("Saved GUID:     " + RUNNING_GUID)
        print("Saved EBAYEPID: " + RUNNING_EBAYEPID)
        print("Current GUID :  " + currentGUID)
        print("Current EPID :  " + currentEPID)
        print("Current Note :  " + str(currentNote))
        print ("------------------------\n\n")
        """
    
    # Print output csv and file
    finalTime = current_milli_time() - forStartTime
    print ("Starting main loop Complete.")
    print ("\nPrinting output...")
    print (outputcsv)
    outputcsv.to_csv(outputFile, index=False);

    print ("\nOutput CSV has been written to: {}".format(outputFile))

    print ("\nTime Taken by main loop: {} milliseconds".format(finalTime))

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

# Running script from command line
if __name__ == "__main__":
    main(sys.argv[1:])