# Hans Fricke 
# 08/08/2017
# python 2.7.11 (64-bit)

# Program updates contact lists for survey distribution and uploads them to Qualtrics
# 1) Fetches the IDs of respondents from SQL database
# 2) Reads in original contact lists, filters out respondents, and saves updated contact lists
# 3) Creates new mailing lists in Qualtrics
# 4) Adds contacts to new mailing lists 

# This is useful if you do not always use the same mode of distributing the survey (e.g Email and TEXT)
# Or if you want to use text reminders

# In this code you need to specify:
'''
- contactsPath 
- sqlDataBase
- numReminder
- contactFileList
- apiToken 
- dataCenter
- category # Folder in qualtrics that will hold the mailing list
- libraryId 
'''
import sqlite3
import os
import pandas as pd
import re
import requests


# ----------------------------------------------------------------------------#
# Set parameters -------------------------------------------------------------#
# ----------------------------------------------------------------------------#

# Path parameters
contactsPath = '----- YOUR FOLDER WITH THE CONTACT LISTS ------'
sqlDataBase = '---- PATH TO THE SQL DATABASE ------'
os.chdir(contactsPath)
numReminder = '---- SOME DISTRIBUTION NAME ----' # Creates a new folder with this name and saves contact lists with this suffix

# Set Qualtrics connection parameters
apiToken = "---- YOUR API TOKEN ----"
dataCenter = "---- YOUR DATA CENTER -----"
category = "---- YOUR CONTACT LIST FOLTER IN QUALTRICS ----"
libraryId = "---- YOUR LIBRARY ID -----"

# list with contact lists to update
contactFileList = [
                    '---- YOUR CONTACT LIST 1.csv ----',
                    '---- YOUR CONTACT LIST 2.csv ----']
 
# ----------------------------------------------------------------------------#
# Programm to filer out responses --------------------------------------------#
# ----------------------------------------------------------------------------#
def filterAnswered(contactData, filterData, saveExtension):
    # Read original contact list
    contacts = pd.read_csv(contactData , encoding = 'latin1', sep = ',',  
                                    low_memory=False, error_bad_lines = False)
    
    # Merge with respondents
    newContacts = contacts.merge(filterData, how='left', left_on='ExternalDataReference', 
                                right_on=0)
    print len(newContacts) - len(contacts)
    
    # Check surveyIDs
    print ''    
    print '----------'
    print contactData
    print newContacts[1].value_counts()

    # Delete those that were successfully merged
    newContacts = newContacts[pd.isnull(newContacts[0])]
    
    # Only keep original columns
    columns=list(contacts.columns.values)
    newContacts = newContacts[columns]
    
    # Create a new folder if not exists
    if not os.path.exists(saveExtension):
        os.makedirs(saveExtension)  

    # Check how many filtered out
    nDeleted = len(contacts)-len(newContacts)

    saveAs = './' + saveExtension +'/' + saveExtension +'_' + contactData
    newContacts.to_csv(saveAs, index=False, encoding = 'latin1')    
        
    return nDeleted

# ----------------------------------------------------------------------------#
# Program to upload contacts to mailinglist in qualtrics ---------------------#
# ----------------------------------------------------------------------------#

def uploadContacts(apiToken, dataCenter, mailingListId, contacts):

	# Set parameters for the Qualtrics API call
    urlContacts = 'https://{0}.qualtrics.com/API/v3/mailinglists/{1}/contacts'.format(dataCenter, mailingListId)
    headersContacts = {
        "content-type": "application/json",
        "x-api-token": apiToken,
        'accept':'application/json',
        }
    
    successCount = 0
    for row in contacts.index:

    	# Create dictionary holing the data to be uploaded to Qualtrics
    	# Each contact is uploaded individually
        firstName = contacts.FirstName[row]
        lastName = contacts.LastName[row] 
        externalDataRef = str(contacts.ExternalDataReference[row])
        email = contacts.PrimaryEmail[row]
        phonenumber = str(contacts.phonenumber[row])
        amount = str(contacts.amount[row])
        textlang = str(contacts.textlang[row])
    
        data = {"firstName": firstName,  
                "lastName": lastName, 
                "email": email,  
                "externalDataRef": externalDataRef, 
                "embeddedData": {  # Embedded data allows you to upload customized data
                    "phonenumber": phonenumber, 
                    "amount": amount, 
                    "textlang": textlang
                }}

        # Email, first name, and last name cannot be past a NaN or missing value
        # Key is dropped in this case        	
        if pd.isnull(email):
            data.pop('email', None)

        if pd.isnull(firstName):
            data.pop('firstName', None)

        if pd.isnull(lastName):
            data.pop('lastName', None)
  
        # Sometimes the connection fails and no json is returned. Try until it works 
        submitted = 0         
        while (submitted < 1):
            RequestResponse = requests.request("POST", urlContacts, json=data, 
                                                        headers=headersContacts)
            try:        
                success = (RequestResponse.json()["meta"]["httpStatus"]=="200 - OK") 
                submitted  = 1                      
            except ValueError:
                continue
        
        successCount = successCount + success 
        if success == False:
            print data
            print RequestResponse.text

    # Return the number of successfully uploaded contacts     
    return successCount 
 
# ----------------------------------------------------------------------------#
# Query SQL database to load responses --------------------------------------#
# ----------------------------------------------------------------------------#

# Connect to SQL database
sqlconn = sqlite3.connect(sqlDataBase)
cur = sqlconn.cursor()

# Select the pseudo ID for all
cur.execute('''SELECT pseudo_id, survey_id FROM Respondent''')    
respondents = cur.fetchall()

# Transform list into data frame
respondents = pd.DataFrame(respondents)

# ----------------------------------------------------------------------------#
# Loop over contact lists, filter out respondents, and save in new folder ----#
# ----------------------------------------------------------------------------#

numDeleted = 0 
for contactFile in contactFileList:
    print contactFile
    numDeleted += filterAnswered(contactFile, respondents, numReminder)

print 'Number of respondents: ', len(respondents), ', number of deleted: ', numDeleted  

# ----------------------------------------------------------------------------#
# Get list of newly created contact list files -------------------------------#
# ----------------------------------------------------------------------------#
filesPath = './' + numReminder
filesList=[os.path.join(subdir, file) for subdir, dirs, files in os.walk(filesPath) 
                for file in files] 


# ----------------------------------------------------------------------------#
# Loop over all the contact list files ---------------------------------------#
# ----------------------------------------------------------------------------#

totalSuccesUpload = 0
for file in filesList:
    # ------------------------------------------------------------------------#
    # Read in information to upload to qualtrics -----------------------------#
    # ------------------------------------------------------------------------#
    contactsPath = file
    
    # Get name for contact list in qualtrics
    mailingListName = re.sub('.csv', '', contactsPath.split('\\')[-1])
    print mailingListName 
    
    # Read in data 
    contacts = pd.read_csv(contactsPath , encoding = 'latin1', sep = ',',  
                                        low_memory=False, error_bad_lines = False)
    
    # ------------------------------------------------------------------------#
    # Create mailing list in qualtrics ---------------------------------------#
    # ------------------------------------------------------------------------#
    
    # Create mailing lst
    urlMailingList = "https://{0}.qualtrics.com/API/v3/mailinglists/".format(dataCenter)
    headersMailingList = {
        "content-type": "application/json",
        "x-api-token": apiToken,
        }
    
    data = '{ "libraryId":"%s", "name":"%s","category":"%s" }' % (libraryId, 
                                                        mailingListName, category)
    
    RequestResponse = requests.request("POST", urlMailingList, data=data, headers=headersMailingList)
    mailingListId = RequestResponse.json()["result"]["id"]
    
    # Keep track of all the contact list IDs created     
    mailingListId_file=open('.\\trackIds_'+numReminder+'.txt', 'a+')
    mailingListId_file.write(mailingListId + "\n")
    mailingListId_file.close()
    
    # ------------------------------------------------------------------------#
    # Read in information to upload to qualtrics -----------------------------#
    # ------------------------------------------------------------------------#
    successfulUploads =  uploadContacts(apiToken=apiToken, dataCenter=dataCenter, 
                                    mailingListId=mailingListId, contacts=contacts)

    print mailingListName, ':', successfulUploads, '/', len(contacts),  'contacts have been uploaded successfully.'
    totalSuccesUpload += successfulUploads

print totalSuccesUpload, 'contacts have been successfully uploaded overall.' 
