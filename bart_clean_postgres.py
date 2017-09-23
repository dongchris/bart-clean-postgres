# SQL BART PROJECT

# Dong, Chris

import psycopg2
import glob
import xlrd
import zipfile
import os
import shutil

#extra libaries
import pandas as pd
import re
from string import digits
import datetime

def ProcessBart(tmpDir, dataDir, SQLConn = None, schema = 'cls', table = 'bart'):
    # if file name does not end in /, then add it to the end of the file name
    if tmpDir[-1]!='/':
        tmpDir = tmpDir + '/'

    #  delete tmpDir if it already exists
    if(os.path.exists(tmpDir)):
        shutil.rmtree(tmpDir)

    # first use regular expressions via glob to search for all files ending in .zip
    # then for each zip file, read it with zipfile.ZipFile function
    # store the result in a list
    zip_ref = [zipfile.ZipFile(z,'r') for z in glob.glob(dataDir + '/*.zip')]

    # for each zip file, extract the contents
    [t.extractall(tmpDir) for t in zip_ref]

    # close each zip file
    [z.close() for z in zip_ref]

    # move the contents inside subfolders to tmpDir
    [shutil.move(x, tmpDir + "/") for x in glob.glob(tmpDir + '/*/*.x*')]

    # remove subfolders (that are now empty)
    [os.rmdir(x) for x in glob.glob(tmpDir + '/*/')]

    # put every file in a list to iterate over
    datalist = glob.glob(tmpDir + '/*')

    # create a dataframe to store the final result
    combineall = pd.DataFrame()

    # outer loop for all (193) files
    for j in range(len(datalist)):

        # open one excel file
        book = xlrd.open_workbook(datalist[j])

        # retrieve sheet name of the excel workbook
        sheetname = book.sheet_names()

        # create a data frame to store the result of one excel workbook
        combine = pd.DataFrame()

        # only get the first three sheets which are WeekDay, Saturday, and Sunday
        for i in range(3):

            # using pandas to read one excel file and the ith sheet
            df = pd.read_excel(datalist[j],
                               sheetname = sheetname[i])

            # from the file name, extract only digits which will give us the year
            year = re.findall(r'\d+', datalist[j])[0]

            # remove anything before / so we only get the name of the file followed by .xls or .xlsx
            month = re.sub(tmpDir + '*/','', datalist[j])

            # remove .xls or .xlsx
            month = re.sub('[.]xls.*','', month)

            # remove any whitespace
            month = re.sub(' ','', month)

            # remove Ridership_
            month = re.sub('Ridership_','', month)

            # remove Entry_ExitMatrices
            month = re.sub('Entry_ExitMatrices', '', month)

            # remove any digits
            month = month.translate(None, digits)

            # changing January to 1, February to 2 etc
            month = datetime.datetime.strptime(month, '%B').month

            # WEEKDAY, SATURDAY, SUNDAY, etc is on the 1st row, 4th column
            daytype = df.columns[3]

            # remove the words ADJUSTED from string
            daytype = daytype.replace("ADJUSTED",'').strip()

            # name of start station
            h = df.iloc[0]

            # remove first row
            df = df[1:]

            # replace column names with name of start station
            df.columns = h

            # get index of Exits column
            removecol = df.columns.get_loc("Exits")

            # keep only columns before Exit column
            df = df.ix[:,0:removecol]

            # retrieve first column which is the name of terminal station
            df.columns.values[0] = 'term'

            # find row number that says Entries
            removerow = df[df['term'] == 'Entries'].index.tolist()[0]

            # keep only columns before Entries row
            df = df.iloc[0:removerow-1]

            # turn dataframe from wide to long
            df = pd.melt(df,
            id_vars = ['term'], var_name = 'start', value_name = 'riders')

            # adding columns for yr, mon, and daytype
            df['yr'] = year
            df['mon'] = month
            df['daytype'] = daytype

            # reordering the columns
            df = df[['mon', 'yr', 'daytype', 'start', 'term', 'riders']]

            # adding to data frame that stores for the excel workbook
            combine = combine.append(df)

            # keeping only the first two characters since sometimes the term or start
            # is written as '12.0' etc.
            combine['term'] = combine['term'].astype(str).str[0:2]
            combine['start'] = combine['start'].astype(str).str[0:2]
        # combine each of the excel workbook which contains 3 sheets each
        combineall = combineall.append(combine)

    # changing the column types
    combineall.astype({'mon':int, 'yr':int, 'daytype':str,
                       'start': str,'term':str, 'riders':float})

    # writing the data frame to a csv file
    combineall.to_csv(tmpDir + "toLoad.csv", index = False)

    SQLCursor = SQLConn.cursor()

    # if there is already a table called cls.bart, drop it
    SQLCursor.execute("""
        DROP TABLE IF EXISTS %s.%s 
    ; """ % (schema, table))

    # create the cls.bart table
    SQLCursor.execute("""
      CREATE TABLE %s.%s
      (
      mon int
    , yr int
    , daytype varchar(15)
    , start varchar(2)
    , term varchar(2)
    , riders float
    ); """ % (schema, table))

    # copy data from toLoad.csv into the table
    SQLCursor.execute("""COPY %s.%s FROM '%s' HEADER CSV; """
                      % (schema, table, tmpDir + 'toLoad.csv'))
    SQLConn.commit()

#######################################################
# enter credentials for postgres
#######################################################
#LCLconnR = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='postgres'")

#ProcessBart("/home/chris/tmp", "/home/chris/BART", SQLConn = LCLconnR, schema = 'cls', table = 'bart')
