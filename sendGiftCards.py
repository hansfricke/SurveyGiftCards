# Hans Fricke 
# 02/13/2017
# python 2.7.11 (64-bit)

# This program requires you to have SQLite installed. You can open the database in the SQLite browser

# Program to send out gift card codes to survey respondents
# 1) Downloads new survey responses from  Qaultrics API 
# 2) Writes the new contact info into SQLite database
# 3) Gets a gift card code from .csv file for each new respondent and adds it to data base
# 4) Sends the gift card codes to new repsondents via email
# 5) repeat 


# In this code you need to specify:
'''
- path
- SurveyExportPath 
- SQLDatabasePath
- SQLBackupPath 
- SQLDatabaseName 
- CODEPath
- surveyIdsDic 
- apiToken 
- dataCenter
- message 
- email address
- smtp host
- reps 
- waittime 
- backupn 
'''



import requests
import zipfile
import pandas as pd
import os
import sqlite3
import datetime
import time
import shutil
import smtplib
import sys
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate

#---------------------------------------------------------------------------------------
#------------------------- Specify paths and parameters --------------------------------
#---------------------------------------------------------------------------------------

# Set path of the main folder 
path='D:/YOUR PROJECT FOLDER'
os.chdir(path)

# Setting path for dataexport and storage
SurveyExportPath = path + '/DownloadFolder'
SQLDatabasePath = path + '/DataBase/'
SQLBackupPath = path + '/DataBase/Archive/'
SQLDatabaseName = 'ResponseTracker.sqlite'

# Set path for files that holds gift card codes
CODEPath = path + '/---Your file with codes---.csv'

# Columns to include when read in data from surveys
pdcolumns = ['ResponseID', 'ExternalDataReference','RecipientEmail']

# List survey is Ids and file extensions in dictionary. Possible to include multiple surveys
surveyIdsDic = {'---Your survey ID 1---': '---Your survey name 1---.csv',
                '---Your survey ID 2---': '---Your survey name 2---.csv',
                '---Your survey ID 3---': '---Your survey name 3---.csv'}
surveyIds = surveyIdsDic.keys()

# Number of repetitions for the loop, time to wait between each iteration in seconds, and frequency of back ups every backupn's iteration
reps = 1000000
waittime = 10
backupn = 120

# Setting user Parameters for Qualtrics API
# Add you Qualtrics token and survey ids
apiToken = "---Your Token---"

fileFormat = "csv"
dataCenter = "---Your data center ---"

# Setting static parameters for Qualtrics API
baseUrl = "https://{0}.qualtrics.com/API/v3/responseexports/".format(dataCenter)
headers = {
    "content-type": "application/json",
    "x-api-token": apiToken,
    }

# Set email message to send the code in in html
def genMessage(code):
    message= """<html> 
             <head></head> 
             <body> 
             <p style="font-weight: bold;">Thank you!<p> 
             <p>
             	Thank you very much for taking the time to complete our survey! 
             	Please accept the electronic gift certificate code below.
             </p>	
             <p style="font-weight: bold; font-size=1.5em; text-align:center;">""" \
                                     + code +  \
             """</p>Thank you again</p> 
             </body>
             </html>"""
    return message
   

#---------------------------------------------------------------------------------------
#----------------------- Set up Email to text connection -------------------------------
#---------------------------------------------------------------------------------------

# Specify you email host and email address
def sendMail(to, subject, text):
    assert type(to)==list

    fro = '---Your email address---' # Add your correspondence email address to send out the code 
    msg = MIMEMultipart()
    msg['From'] = fro
    msg['To'] = COMMASPACE.join(to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text, 'html'))
    smtp = smtplib.SMTP('---Your mail host---') # Add your SMTP Host 
    smtp.sendmail(fro, to, msg.as_string() )
    smtp.close()

   
#---------------------------------------------------------------------------------------
#----------------------- Define program for data base back up --------------------------
#---------------------------------------------------------------------------------------

def createBackup(path, database):
    
    # Check if the provided path is valid
    if not os.path.isdir(path):
        raise Exception("Backup directory does not exist: {}".format(path))
    
    # Define file name for the back up, includes date and time
    backup_file = os.path.join(path, 'backup' +
                   	time.strftime("-%Y%m%d-%H%M")+ '.sqlite')
    
    # Lock database before making a backup
    cur.execute('begin immediate')
    # Make new backup file
    shutil.copyfile(database, backup_file)
    print ("\nCreating {}...".format(backup_file))
    # Unlock database
    sqlconn.rollback()    

#---------------------------------------------------------------------------------------
#----------------------------- Setting up data base ------------------------------------
#---------------------------------------------------------------------------------------

# SQL schema

# Data base pathe + file name
database = SQLDatabasePath+SQLDatabaseName

# Connect to SQLite API
sqlconn = sqlite3.connect(database)
cur = sqlconn.cursor()

# Execute SQL code to create new data base with a table for respondents and for surveys
# If data base and these tables already exist, nothing will happen
cur.executescript('''

    CREATE TABLE IF NOT EXISTS Survey (
        id   INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name  TEXT UNIQUE
    );
    
    CREATE TABLE IF NOT EXISTS Respondent (
        id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        individual_id  INTEGER UNIQUE,
        response_id TEXT UNIQUE,
        survey_id   INTEGER,
        email TEXT UNIQUE,
        create_date DATETIME DEFAULT (DATETIME(CURRENT_TIMESTAMP, 'LOCALTIME')),
        redeem_code TEXT UNIQUE,
        sentstatus INTEGER,
        sent_date  DATETIME   
        )

''')

# Commit (save) changes to data base
sqlconn.commit()    

#---------------------------------------------------------------------------------------
#------------------------------ Setting up loop ----------------------------------------
#---------------------------------------------------------------------------------------

# Everything below will be repeated for the specified number of iterations
for t in xrange(reps):

    # Provide some information in the console
    print "----------"
    print "Iteration :" , t, "Date: ", datetime.datetime.now().strftime("%m/%d/%y %H:%M:%S") 
    
    #---------------------------------------------------------------------------------------
    #------------------------------ 1) Download surveys ------------------------------------
    #---------------------------------------------------------------------------------------

    # Delete all files in extraction path, in case they don't get downloaded in this iteration
    deletelist = [os.path.join(subdir, file) for subdir, dirs, files in os.walk(SurveyExportPath) 
                    for file in files]
    for file in deletelist:
        os.remove(file)


    # Iterate over survey IDs to download each one seperately
    for surveyId in surveyIds:
    
        survey = surveyIdsDic[surveyId] # Identify the survey

        try:
            # Fetch last response id from database used to download only new responses
            cur.execute('''SELECT response_id FROM Respondent 
                                WHERE id == (SELECT max(id) FROM Respondent 
                                    WHERE survey_id == (SELECT id FROM Survey WHERE name == ? )) ''', (survey,))
    
            lastResponseId=cur.fetchone()[0]
            
            # Set parameters to send to Qualtrics API
            downloadRequestPayload = '{"format":"' + fileFormat + '","surveyId":"' + surveyId + '","lastResponseId":"' + lastResponseId +'"}'

        # Set exception for case that noone has answered to this survey yet
        except (TypeError, sqlconn.OperationalError) as e: 
            print e
            
            # Set parameters without specifing last response id (all responses will be downloaded)
            downloadRequestPayload = '{"format":"' + fileFormat + '","surveyId":"' + surveyId +'"}'

        downloadRequestUrl = baseUrl

        try:
            # Connect to Qualtrics API and send download request
            downloadRequestResponse = requests.request("POST", downloadRequestUrl, data=downloadRequestPayload, headers=headers)
            progressId = downloadRequestResponse.json()["result"]["id"]
            
            # Checking on data export progress and waiting until export is ready
            startlooptime = time.time() # Record time to make sure the loop doesn't run forever
            requestCheckProgress = 0
            
            # As long as export not complete keep checking
            while requestCheckProgress < 100:
                requestCheckUrl = baseUrl + progressId
                requestCheckResponse = requests.request("GET", requestCheckUrl, headers=headers) 
                requestCheckProgress = requestCheckResponse.json()["result"]["percentComplete"]        

                complete = 1
                
                # Check how long loop has been running
                looptime = time.time() - startlooptime               
                if looptime/60 > 3: # Abort if download takes more than 3 minutes
                    print "Download took more than three minutes. Try again next itereation."
                    complete = 0    
                    break

            # If export complete, download and unzip file
            if complete==1:
                requestDownloadUrl = baseUrl + progressId + '/file'
                requestDownload = requests.request("GET", requestDownloadUrl, headers=headers, stream=True)
                with open("RequestFile.zip", "wb+") as f:
                    for chunk in requestDownload.iter_content(chunk_size=1024):
                        f.write(chunk)

        except (KeyError, requests.ConnectionError, ValueError, IOError) as e: # Something went wrong with the Qualtrics API (retry)
            print "Survey not downloaded: ", e
            continue        

        try:
            zipfile.ZipFile("RequestFile.zip").extractall(SurveyExportPath)

        except (zipfile.BadZipfile, IOError) as e:
            print SurveyExportPath
            print "Zipfile not extracted: ", e                
            continue
    
  
    
    #---------------------------------------------------------------------------------------
    #---------------------- 2) Populating database with survey data ------------------------
    #---------------------------------------------------------------------------------------
    
    
    SurveyFileList = [os.path.join(subdir, file) for subdir, dirs, files in os.walk(SurveyExportPath) 
                    for file in files]
    
    for file in SurveyFileList:
        data=pd.read_csv(file, encoding = 'utf-8-sig', sep = ',',
                            usecols = pdcolumns,   
                            low_memory=False, error_bad_lines = False)
        data=data.iloc[2:,:].reset_index(drop=True)
          
        survey = file.split('\\')[-1] # Identify the survey

        if len(data.index)>0: # Only if new responses are recorded
            for row in xrange(0, len(data.index)):
              
                individual_id = data.loc[row, 'ExternalDataReference']
                response_id = data.loc[row, 'ResponseID']
                email = data.loc[row, 'RecipientEmail']
        
                # Record the survey name
                cur.execute(''' INSERT or IGNORE INTO Survey
                                (name)
                                VALUES (?)''',
                                (survey,)) 
        
                # Fetch survey id to enter in response table
                cur.execute('''SELECT id FROM Survey WHERE name == ? ''', (survey,))
                survey_id=cur.fetchone()[0]
                    
                cur.execute(''' INSERT or IGNORE INTO Respondent
                                (email, individual_id, response_id, survey_id)
                                VALUES (?,?,?,?)''',
                                (email, individual_id, response_id, survey_id))    
                sqlconn.commit() 
        
    #---------------------------------------------------------------------------------------
    #------------------------- 3) Get gift card code from CSV file -------------------------
    #---------------------------------------------------------------------------------------
    
    # Select new respondents who need a code     
    cur.execute('''SELECT id FROM Respondent WHERE redeem_code IS NULL ''')
    NeedGiftCards = cur.fetchall()
    
    numCodesAssigned = 0 # Count number of the codes assigned  
    if len(NeedGiftCards)>0: #only if new respondents

        # Import csv file that holds the codes
        allcodes=pd.read_csv(CODEPath, encoding = 'utf-8-sig', sep = ',',  
                                low_memory=False, error_bad_lines = False)

        # Identify last redeem code used
        try:
            cur.execute('''SELECT redeem_code FROM Respondent 
                                WHERE id == (SELECT max(id) FROM Respondent
                                WHERE redeem_code IS NOT NULL)''')

            lastcode = cur.fetchone()[0]
            row=allcodes[allcodes.code==lastcode].index.values[0] # Get index value for last code
            
        except TypeError:
            row = -1

        usecodes=allcodes[allcodes.index>row] # Select all codes after that value

        for needcard in NeedGiftCards:
            row +=1 
            # Extract data
            sqlDB_id = needcard[0]
            redeem_code=usecodes.code[row]  
            numCodesAssigned += 1   

            # Add code to SQL database    
            cur.execute(''' UPDATE  Respondent 
                            SET  redeem_code = ?
                            WHERE id == ?''',
                            (redeem_code, sqlDB_id))
            
            sqlconn.commit()  

    print 'Number of gift card codes assigned:',  numCodesAssigned       
    #---------------------------------------------------------------------------------------
    #---------------------------- 4) Send Code with Email  ---------------------------------
    #---------------------------------------------------------------------------------------
    # Getting all contacts and codes for which the code has not been sent    
    cur.execute('''SELECT id, email, redeem_code FROM Respondent 
                    WHERE redeem_code IS NOT NULL and sentstatus IS NULL''')

    contacts = cur.fetchall()
   
    numCodesSent = 0 # Count the number of codes sent
    if len(contacts)>0: # Only new respondents

        for contact in contacts:

            sqlDB_id = contact[0]
            email = contact[1]
            code = contact[2]
    
            message = genMessage(code)

            TOADDR  = [email]          

            # Send message
            try:
                sendMail(TOADDR, "Thank you for your participation!", message)
                numCodesSent += 1
                sentstatus = 1
                cur.execute(''' UPDATE Respondent 
                                SET sentstatus = ?,
                                sent_date=datetime(CURRENT_TIMESTAMP, 'localtime')
                                WHERE id == ?''',
                                (sentstatus, sqlDB_id))
    
                sqlconn.commit() 

            except:
                e = sys.exc_info()[0]
                print "Error:",  e 
                continue

    print 'Number of codes sent:', numCodesSent
    #---------------------------------------------------------------------------------------
    #--------------------------   Save backup copy every backupn's time  -----------------
    #---------------------------------------------------------------------------------------
    
    if t % backupn == 0:
        """Create timestamped database copy"""
        createBackup(SQLBackupPath, database)

    time.sleep(waittime)

# Close SQL connection
sqlconn.close()


