# Hans Fricke 
# 02/13/2017
# python 2.7.11 (64-bit)

# This program requires you to have SQLite installed. You can open the database in the SQLite browser

# Program to send out gift card codes to survey respondents
# 1) Downloads new survey responses from  Qualtrics API 
# 2) Writes the new contact info into SQLite database
# 3) Request a gift card code from Amazon Incentives API for each new respondent and adds it to data base
#    (This code is for the sandbox environment, for production environment change host)
# 4) Sends the gift card codes to new repsondents via EZtexting.com API 
# 5) repeat 

# In this code you need to specify:
'''
- path
- SurveyExportPath 
- SQLDatabasePath
- SQLBackupPath 
- SQLDatabaseName 
- surveyIdsDic 
- apiToken 
- dataCenter 
- ezUserName 
- ezPassword 
- Need to download "https://curl.haxx.se/docs/caextract.html" and put in working directory
- message
- reps 
- waittime 
- backupn 
- awsKeyID 
- awsSecretKey 
- partnerID
'''

import requests
import zipfile
import pandas as pd
import os
import sqlite3
import binascii
import datetime
import hmac, hashlib
import json
import urllib2
import xml.etree.ElementTree as ElementTree
import pycurl
import StringIO
import time
import shutil


#---------------------------------------------------------------------------------------
#------------------------- Specify paths and parameters --------------------------------
#---------------------------------------------------------------------------------------

# Set path of the main folder 
path='--- YOUR WORKING DIRECTORY ----'
os.chdir(path)

# Setting path for dataexport and storage
SurveyExportPath = path + '/DownloadFolder'
SQLDatabasePath = path + '/DataBase/'
SQLBackupPath = path + '/DataBase/Archive/'
SQLDatabaseName = 'ResponseTracker.sqlite'


# Columns to include when read in data
pdcolumns = ['ResponseID', 'ExternalDataReference', 'phonenumber','textlang', 'amount']

#Setting user Parameters for Qualtrics API
apiToken = "---Your Token---"

fileFormat = "csv"
dataCenter = "---Your data center ---"

surveyIdsDic = {'---Your survey ID 1---': '---Your survey name 1---.csv',
                '---Your survey ID 2---': '---Your survey name 2---.csv',
                '---Your survey ID 3---': '---Your survey name 3---.csv'}
surveyIds = surveyIdsDic.keys()

baseUrl = "https://{0}.qualtrics.com/API/v3/responseexports/".format(dataCenter)
headers = {
    "content-type": "application/json",
    "x-api-token": apiToken,
    }

# Setting up parameters for EZtexting API
curl = pycurl.Curl()
curl.setopt(pycurl.SSL_VERIFYPEER, 1)
curl.setopt(pycurl.SSL_VERIFYHOST, 2)

permPath = '{0}/cacert.pem.txt'.format(path) # Need to download "https://curl.haxx.se/docs/caextract.html"
curl.setopt(pycurl.CAINFO, permPath)

# Declare EZtexting username and password
ezUserName = "----YOUR USERNAME ----"
ezPassword = "----YOUR PASSWORD ----"
EZUserPassword="User={0}&Password={1}".format(ezUserName, ezPassword)

# number of repititions, time to wait between each iteration in seconds, and frequency of back ups every backupn's iteration
reps = 80000
waittime = 15
backupn = 15


#---------------------------------------------------------------------------------------
#-------------- Define a function that constructs the EZtexting message ----------------
#---------------------------------------------------------------------------------------
def createEzMessage(code, amount, language):
    if language == 2:      # Spanish  
        message = "&Message=Gracias! Aqui esta su tarjeta de Amazon.com por $" + str(amount) + \
                    " Codigo de Regalo: " + code + \
                    ". Aplica restricciones vea en amazon.com/gc-legal"
    else:
        message = "&Message=Thank you for your feedback! Here is your $" + str(amount) + \
                    " Amazon.com Gift Card code: " + code + \
                        ". Restrictions apply see amazon.com/gc-legal"
    return message   


#---------------------------------------------------------------------------------------
#----------------------- Define program for database back up ---------------------------
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
#----------------------- Set up Amazon API connection ----------------------------------
#---------------------------------------------------------------------------------------

# Most of this is adapted code provided by Amazon

class AGCODServiceOperation:
    '''
    An enumeration of the types of API this sample code supports
    '''
    ActivateGiftCard, DeactivateGiftCard, ActivationStatusCheck, CreateGiftCard, CancelGiftCard, GetGiftCardActivityPage = range(6)
    @classmethod
    def tostring(cls, val):
        for k,v in vars(cls).iteritems():
            if v == val:
                return k

class PayloadType:
    '''
    An enumeration of supported formats for the payload
    '''
    JSON, XML = range(2)
    @classmethod
    def tostring(cls, val):
        for k,v in vars(cls).iteritems():
            if v == val:
                return k

class AppConstants:
    """
    forbids to overwrite existing variables
    forbids to add new values if 'locked' variable exists
    """
    def __setattr__(self,name,value):
        if(self.__dict__.has_key("locked")):
            raise NameError("Class is locked can not add any attributes (%s)" % name)
        if self.__dict__.has_key(name):
            raise NameError("Can't rebind const(%s)" % name)
        self.__dict__[name]=value

    #Static headers used in the request
    ACCEPT_HEADER = "accept"
    CONTENT_HEADER = "content-type"
    HOST_HEADER = "host"
    XAMZDATE_HEADER = "x-amz-date"
    XAMZTARGET_HEADER = "x-amz-target"
    AUTHORIZATION_HEADER = "Authorization"

    #Static format parameters
    DATE_FORMAT = "%Y%m%dT%H%M%SZ"
    DATE_TIMEZONE = "UTC"
    UTF8_CHARSET = "UTF-8"

    #Signature calculation related parameters
    HMAC_SHA256_ALGORITHM = "HmacSHA256"
    HASH_SHA256_ALGORITHM = "SHA-256"
    AWS_SHA256_ALGORITHM = "AWS4-HMAC-SHA256"
    KEY_QUALIFIER = "AWS4"
    TERMINATION_STRING = "aws4_request"

    #User and instance parameters
    awsKeyID = "-- YOUR AWS KEY HERE --" # Your KeyID
    awsSecretKey = "-- YOUR AWS SECRET KEY HERE --" # Your Key
    dateTimeString = ""   #"20140630T224526Z"

    #Service and target (API) parameters
    regionName = "us-east-1" #lowercase!  Ref http://docs.aws.amazon.com/general/latest/gr/rande.html
    serviceName = "AGCODService"

    #Payload parameters
    partnerID = "--YOUR PARTNER ID HERE --" # Your Partner ID
    requestID = ""
    cardNumber = ""
    amount = ""
    currencyCode = "USD"

    #Additional payload parameters for CancelGiftCard
    gcId = ""

    #Additional payload parameters for GetGiftCardActivityPage
    pageIndex = 0
    pageSize = 1
    utcStartDate = "" #"yyyy-MM-ddTHH:mm:ss eg. 2013-06-01T23:10:10"
    utcEndDate = "" #"yyyy-MM-ddTHH:mm:ss eg. 2013-06-01T23:15:10"
    showNoOps = True

    #Parameters that specify what format the payload should be in and what fields will
    #be in the payload, based on the selected operation.
    msgPayloadType = PayloadType.XML
    #msgPayloadType = PayloadType.JSON
    serviceOperation = AGCODServiceOperation.CreateGiftCard
    #serviceOperation = AGCODServiceOperation.CancelGiftCard
    #serviceOperation = AGCODServiceOperation.ActivateGiftCard
    #serviceOperation = AGCODServiceOperation.DeactivateGiftCard
    #serviceOperation = AGCODServiceOperation.ActivationStatusCheck
    #serviceOperation = AGCODServiceOperation.GetGiftCardActivityPage

    #Parameters used in the message header
    host = "agcod-v2-gamma.amazon.com" #Refer to the AGCOD tech spec for a list of end points based on region/environment
    protocol = "https"
    queryString = ""    # empty
    requestURI = "/" + AGCODServiceOperation.tostring(serviceOperation)
    serviceTarget = "com.amazonaws.agcod.AGCODService" + "." + AGCODServiceOperation.tostring(serviceOperation)
    hostName = protocol + "://" + host + requestURI


class Dict2Tree(dict):
    '''
    Builder of an ElementTree from a dict with a single key and a value that may be a dict of dicts.
    @param aDict the input dictionary
    '''
    def __init__(self, aDict):
        if not aDict or len(aDict.items()) != 1:
            raise Exception("IllegalArgumentException")
        top_key = aDict.keys()[0]
        self.root = ElementTree.Element(top_key)
        self.addChildren(self.root, aDict[top_key])
    def addChildren(self, node, structure):
        if type(structure) is dict:
            for key in structure:
                child = ElementTree.SubElement(node, key)
                self.addChildren(child, structure[key])
        elif type(structure) is bool:
            node.text = str(structure).lower()
        else:
            node.text = str(structure)
    def tostring(self):
        return ElementTree.tostring(self.root, 'utf-8')


def buildPayloadContent():
    '''
    Creates a dict containing the data to be used to form the request payload.
    @return the populated dict of data
    '''
    params = {"partnerId" : app.partnerID}
    if app.serviceOperation == AGCODServiceOperation.ActivateGiftCard:
        params["activationRequestId"] = app.requestID
        params["cardNumber"]   = app.cardNumber
        params["value"]        = {"currencyCode" : app.currencyCode, "amount" : app.amount}

    elif app.serviceOperation == AGCODServiceOperation.DeactivateGiftCard:
        params["activationRequestId"] = app.requestID
        params["cardNumber"]   = app.cardNumber

    elif app.serviceOperation == AGCODServiceOperation.ActivationStatusCheck:
        params["statusCheckRequestId"] = app.requestID
        params["cardNumber"]   = app.cardNumber

    elif app.serviceOperation == AGCODServiceOperation.CreateGiftCard:
        params["creationRequestId"] = app.requestID
        params["value"]        = {"currencyCode" : app.currencyCode, "amount" : app.amount}

    elif app.serviceOperation == AGCODServiceOperation.CancelGiftCard:
        params["creationRequestId"] = app.requestID
        params["gcId"]         = app.gcId

    elif app.serviceOperation == AGCODServiceOperation.GetGiftCardActivityPage:
        params["requestId"]    = app.requestID
        params["utcStartDate"] = app.utcStartDate
        params["utcEndDate"]   = app.utcEndDate
        params["pageIndex"]    = app.pageIndex
        params["pageSize"]     = app.pageSize
        params["showNoOps"]    = app.showNoOps

    else:
        raise Exception("IllegalArgumentException")

    return {AGCODServiceOperation.tostring(app.serviceOperation) + "Request" : params}


def setPayload():
    '''
    Sets the payload to be the requested encoding and creates the payload based on the static parameters.
    @return A tuple including the payload to be sent to the AGCOD service and the content type
    '''
    #Set payload based on operation and format
    payload_dict = buildPayloadContent()
    if app.msgPayloadType == PayloadType.XML:
        contentType = "charset=UTF-8"
        payload = Dict2Tree(payload_dict).tostring()
    elif app.msgPayloadType == PayloadType.JSON:
        contentType = "application/json"
        # strip operation specifier from JSON payload
        payload = json.dumps(payload_dict[payload_dict.keys()[0]])
    else:
        raise Exception("IllegalPayloadType")
    return payload, contentType


def buildCanonicalRequest(payload, contentType):
    '''
    Creates a canonical request based on set static parameters
    http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html

    @param payload - The payload to be sent to the AGCOD service
    @param contentType - the wire format of content to be posted
    @return The whole canonical request string to be used in Task 2
    '''

    #Create a SHA256 hash of the payload, used in authentication
    payloadHash = hashstr(payload)

    #Canonical request headers should be sorted by lower case character code
    canonicalRequest = "POST\n" \
        + app.requestURI + "\n" \
        + app.queryString + "\n" \
        + app.ACCEPT_HEADER + ":" + contentType + "\n" \
        + app.CONTENT_HEADER + ":" + contentType + "\n" \
        + app.HOST_HEADER + ":" + app.host + "\n" \
        + app.XAMZDATE_HEADER + ":" + app.dateTimeString + "\n" \
        + app.XAMZTARGET_HEADER + ":" + app.serviceTarget + "\n" \
        + "\n" \
        + app.ACCEPT_HEADER + ";" + app.CONTENT_HEADER + ";" + app.HOST_HEADER + ";" + app.XAMZDATE_HEADER + ";" + app.XAMZTARGET_HEADER + "\n" \
        + payloadHash
    return canonicalRequest


def buildStringToSign(canonicalRequestHash, dateString):
    '''
    Uses the previously calculated canonical request to create a single "String to Sign" for the request
    http://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html

    @param canonicalRequestHash - SHA256 hash of the canonical request
    @param dateString - The short 8 digit format for an x-amz-date
    @return The "String to Sign" used in Task 3
    '''
    stringToSign = app.AWS_SHA256_ALGORITHM + "\n" \
        + app.dateTimeString + "\n" \
        + dateString + "/" + app.regionName + "/" + app.serviceName + "/" + app.TERMINATION_STRING + "\n" \
        + canonicalRequestHash
    return stringToSign


def hmac_binary(data, bkey):
    '''
    Create a series of Hash-based Message Authentication Codes for use in the final signature

    @param data - String to be Hashed
    @param bkey - Key used in signing
    @return Byte string of resultant hash
    '''
    return hmac.new(bkey, data, hashlib.sha256).digest()


def buildDerivedKey(dateString):
    '''
    This function uses given parameters to create a derived key based on the secret key and parameters related to the call
    http://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html

    @param dateString - The short 8 digit format for an x-amz-date
    @return The derived key used in creating the final signature
    '''

    signatureAWSKey = app.KEY_QUALIFIER + app.awsSecretKey

    #Calculate the derived key from given values
    derivedKey = hmac_binary(app.TERMINATION_STRING, \
            hmac_binary(app.serviceName, \
            hmac_binary(app.regionName, \
            hmac_binary(dateString, signatureAWSKey))))
    return derivedKey


def buildAuthSignature(stringToSign, dateString):
    '''
    Calculates the signature to put in the POST message header 'Authorization'
    http://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html

    @param stringToSign - The entire "String to Sign" calculated in Task 2
    @param dateString - The short 8 digit format for an x-amz-date
    @return The whole field to be used in the Authorization header for the message
    '''

    #Use derived key and "String to Sign" to make the final signature
    derivedKey = buildDerivedKey(dateString)

    finalSignature = hmac_binary(stringToSign, derivedKey)

    signatureString = binascii.hexlify(finalSignature)
    authorizationValue = app.AWS_SHA256_ALGORITHM \
        + " Credential=" + app.awsKeyID + "/" \
        + dateString + "/" \
        + app.regionName + "/" \
        + app.serviceName + "/" \
        + app.TERMINATION_STRING + "," \
        + " SignedHeaders=" + app.ACCEPT_HEADER + ";" \
        + app.CONTENT_HEADER + ";" \
        + app.HOST_HEADER + ";" \
        + app.XAMZDATE_HEADER + ";" \
        + app.XAMZTARGET_HEADER + "," \
        + " Signature=" + signatureString

    return authorizationValue


def hashstr(toHash):
    '''
    Used to hash the payload and hash each previous step in the AWS signing process

    @param toHash - String to be hashed
    @return SHA256 hashed version of the input
    '''
    return hashlib.sha256(toHash).hexdigest()


def printRequestInfo(payload, canonicalRequest, canonicalRequestHash, stringToSign, authorizationValue, dateString, contentType):
    '''
    Creates a printout of all information sent to the AGCOD service

    @param payload - The payload to be sent to the AGCOD service
    @param canonicalRequest - The entire canonical request calculated in Task 1
    @param canonicalRequestHash - SHA256 hash of canonical request
    @param stringToSign - The entire "String to Sign" calculated in Task 2
    @param authorizationValue - The entire authorization calculated in Task 3
    @param dateString - The short 8 digit format for an x-amz-date
    @param contentType - the wire format of content to be posted
    '''

    #Print everything to be sent:
    print "\nPAYLOAD:"
    print payload
    print "\nHASHED PAYLOAD:"
    print hashstr(payload)
    print "\nCANONICAL REQUEST:"
    print canonicalRequest
    print "\nHASHED CANONICAL REQUEST:"
    print canonicalRequestHash
    print "\nSTRING TO SIGN:"
    print stringToSign
    print "\nDERIVED SIGNING KEY:"
    print binascii.hexlify(buildDerivedKey(dateString))
    print "\nSIGNATURE:"

    #Check that the signature is moderately well formed to do string manipulation on
    if authorizationValue.find("Signature=") < 0 or authorizationValue.find("Signature=") + 10 >= len(authorizationValue):
        raise Exception("Malformed Signature")

    #Get the text from after the word "Signature=" to the end of the authorization signature
    print authorizationValue[authorizationValue.find("Signature=") + 10:]
    print "\nENDPOINT:"
    print app.host
    print "\nSIGNED REQUEST"
    print "POST " + app.requestURI + " HTTP/1.1"
    print app.ACCEPT_HEADER + ":" + contentType
    print app.CONTENT_HEADER + ":" + contentType
    print app.HOST_HEADER + ":" + app.host
    print app.XAMZDATE_HEADER + ":" + app.dateTimeString
    print app.XAMZTARGET_HEADER + ":" + app.serviceTarget
    print app.AUTHORIZATION_HEADER + ":" + authorizationValue
    print payload


def signRequestAWSv4(conn, payload, contentType):
    '''
    Creates the authentication signature used with AWS v4 and sets the appropriate properties within the connection
    based on the parameters used for AWS signing. Tasks described below can be found at
    http://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html

    @param conn - URL connection to host
    @param payload - The payload to be sent to the AGCOD service
    @param contentType - the wire format of content to be posted
    '''
    if conn == None:
        raise Exception("ConnectException")

    #Convert full date to x-amz-date by ignoring fields we don't need
    #dateString only needs digits for the year(4), month(2), and day(2).
    dateString = app.dateTimeString[0 : 8]

    #Set proper request properties for the connection, these correspond to what was used creating a canonical request
    #and the final Authorization
    conn.add_header(app.ACCEPT_HEADER, contentType)
    conn.add_header(app.CONTENT_HEADER, contentType)
    conn.add_header(app.HOST_HEADER, app.host)
    conn.add_header(app.XAMZDATE_HEADER, app.dateTimeString)
    conn.add_header(app.XAMZTARGET_HEADER, str(app.serviceTarget))

    #Begin Task 1: Creating a Canonical Request
    canonicalRequest = buildCanonicalRequest(payload, contentType)
    canonicalRequestHash = hashstr(canonicalRequest)

    #Begin Task 2: Creating a String to Sign
    stringToSign = buildStringToSign(canonicalRequestHash, dateString)

    #Begin Task 3: Creating a Signature
    authorizationValue = buildAuthSignature(stringToSign, dateString)

    #set final connection header
    conn.add_header(app.AUTHORIZATION_HEADER, authorizationValue)

    ''' disabled so it wont print evrything'''
    #Print everything to be sent:
    #printRequestInfo(payload, canonicalRequest, canonicalRequestHash, stringToSign, authorizationValue, dateString, contentType)


#---------------------------------------------------------------------------------------
#----------------------------- Setting up database ------------------------------------
#---------------------------------------------------------------------------------------

# General architecture

database = SQLDatabasePath+SQLDatabaseName
sqlconn = sqlite3.connect(database)
cur = sqlconn.cursor()

cur.executescript('''

    CREATE TABLE IF NOT EXISTS Survey (
        id   INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name  TEXT UNIQUE
    );
    
    CREATE TABLE IF NOT EXISTS Respondent (
        id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        pseudo_id  INTEGER UNIQUE,
        response_id INTEGER UNIQUE,
        survey_id   INTEGER,
        number   INTEGER UNIQUE,
        language INTEGER,
        amount INTEGER,
        create_date DATETIME DEFAULT (DATETIME(CURRENT_TIMESTAMP, 'LOCALTIME')),
        redeem_code TEXT UNIQUE,
        request_id  TEXT UNIQUE,
        giftcard_id  TEXT UNIQUE,
        return_amount FLOAT,
        sentstatus INTEGER,
        sent_date  DATETIME   
        )

''')

sqlconn.commit()    

#---------------------------------------------------------------------------------------
#------------------------------ seeting up time loop ---------------------------------------
#---------------------------------------------------------------------------------------

for t in xrange(reps):

    # Provide some information
    print "----------"
    print "Iteration :" , t, "Date: ", datetime.datetime.now().strftime("%m/%d/%y %H:%M:%S") 
    
    #---------------------------------------------------------------------------------------
    #------------------------------ Download surveys ---------------------------------------
    #---------------------------------------------------------------------------------------
    
    # Delete all files in extraction path
    deletelist = [os.path.join(subdir, file) for subdir, dirs, files in os.walk(SurveyExportPath) 
                    for file in files]
    for file in deletelist:
        os.remove(file)

    
    for surveyId in surveyIds:
    
        # Query last response id for each survey
        survey = surveyIdsDic[surveyId] # Identify the survey

                     
        try:
            cur.execute('''SELECT response_id FROM Respondent 
                                WHERE id == (SELECT max(id) FROM Respondent 
                                    WHERE survey_id == (SELECT id FROM Survey WHERE name == ? )) ''', (survey,))
    
            lastResponseId=cur.fetchone()[0]
            downloadRequestPayload = '{"format":"' + fileFormat + '","surveyId":"' + surveyId + '","lastResponseId":"' + lastResponseId +'"}'
                                      
        except (TypeError, sqlconn.OperationalError) as e: # if data base empty this will be an error
            print e
            downloadRequestPayload = '{"format":"' + fileFormat + '","surveyId":"' + surveyId +'"}'

        downloadRequestUrl = baseUrl
        print downloadRequestPayload
        try:
            downloadRequestResponse = requests.request("POST", downloadRequestUrl, data=downloadRequestPayload, headers=headers)
            progressId = downloadRequestResponse.json()["result"]["id"]
            
            # Checking on data export progress and waiting until export is ready
            startlooptime = time.time() # Make sure it's not caught in this loop
            requestCheckProgress = 0
            while requestCheckProgress < 100:
                requestCheckUrl = baseUrl + progressId
                requestCheckResponse = requests.request("GET", requestCheckUrl, headers=headers) 
                requestCheckProgress = requestCheckResponse.json()["result"]["percentComplete"]
   
                complete = 1
                looptime = time.time() - startlooptime               
                if looptime/60 > 3: # If download takes too long -> abort
                    print "Download took more than three minutes. Try again next itereation."
                    complete = 0    
                    break

            # Downloadand unzip file
            if complete==1:
                requestDownloadUrl = baseUrl + progressId + '/file'
                requestDownload = requests.request("GET", requestDownloadUrl, headers=headers, stream=True)
                with open("RequestFile.zip", "wb") as f:
                    for chunk in requestDownload.iter_content(chunk_size=1024):
                        f.write(chunk)

        except (KeyError, requests.ConnectionError, ValueError, IOError) as e: # Something went wrong with the Qualtrics API (retry)
            print "Survey not downloaded: ", e
            continue        

        try:
            zipfile.ZipFile("RequestFile.zip").extractall(SurveyExportPath)
        except (zipfile.BadZipfile, IOError) as e:
            print "Zipfile not extracted: ", e                
            continue
    
  
    
    #---------------------------------------------------------------------------------------
    #---------------------- Populating database with survey data ---------------------------
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

                try:
                    number = int(filter(str.isdigit, str(data.loc[row, 'phonenumber'])))
                except ValueError: 
                    continue
                
                pseudo_id = data.loc[row, 'ExternalDataReference']
                response_id = data.loc[row, 'ResponseID']
                language = data.loc[row, 'textlang']
                amount = data.loc[row, 'amount']
        
                # Record the survey name
                cur.execute(''' INSERT or IGNORE INTO Survey
                                (name)
                                VALUES (?)''',
                                (survey,)) 
        
                # Query survey id to enter in response table
                cur.execute('''SELECT id FROM Survey WHERE name == ? ''', (survey,))
                survey_id=cur.fetchone()[0]
                    
                cur.execute(''' INSERT or IGNORE INTO Respondent
                                (number, pseudo_id, response_id, survey_id, language, amount)
                                VALUES (?,?,?,?,?,?)''',
                                (number, pseudo_id, response_id, survey_id, language, amount))    
                sqlconn.commit() 
        
    #---------------------------------------------------------------------------------------
    #------------------------------- Request gift card codes -------------------------------
    #---------------------------------------------------------------------------------------
    
    # Only for those that don't have a code yet
    
    cur.execute('''SELECT id, amount FROM Respondent WHERE redeem_code IS NULL ''')    
    woGiftCards = cur.fetchall()

    NsuccessfullCards = 0 # Count successfull uploads
    if len(woGiftCards)>0: # Only if new respondents
        
        
        for woGiftCard in woGiftCards:

            # extract data
            sqlDB_id = woGiftCard[0]
            amount = woGiftCard[1]    
                
        
            # Request gift card parameters------------------------------------------------------
            request_id = AppConstants.partnerID + datetime.datetime.now().strftime("%y%m%d") + str(sqlDB_id)
        
            app = AppConstants()
            app.requestID = request_id 
            app.amount = amount
            app.dateTimeString = datetime.datetime.utcnow().strftime(app.DATE_FORMAT)
    
            # Initialize whole payload in the specified format for the given operation and set additional headers based on these settings
            payload, contentType = setPayload()
            
            # Create the URL connection to the host
            hostConnection = app.hostName
            conn = urllib2.Request(url=hostConnection)

            # Create Authentication signature and set the connection parameters
            signRequestAWSv4(conn, payload, contentType)            
            try:

                # Write the output message to the connection, if it gives an errors, it will generate an IOException
                conn.add_data(payload)
                outdata = conn.get_data() 
            
                req = urllib2.urlopen(conn)
                response = req.read()
                req.close()
            
                responseContent = ElementTree.fromstring(response)
                redeem_code = responseContent.findtext('.//gcClaimCode')
                giftcard_id = responseContent.findtext('.//gcId')
                return_amount = responseContent.findtext('.//amount')
                
                cur.execute(''' UPDATE Respondent 
                            SET request_id = ?, redeem_code = ?, giftcard_id = ?, return_amount =?
                            WHERE id == ?''',
                            (request_id, redeem_code, giftcard_id, return_amount, sqlDB_id))
            
                sqlconn.commit()  
                NsuccessfullCards += 1
        
            except urllib2.HTTPError as e:
                    # If the server returns an unsuccessful HTTP status code, such as 3XX, 4XX and 5XX, an HTTPError exception is thrown.
                print 'OUTGOING DATA:'
                print outdata
                print "\nERROR RESPONSE:"
                print response, e.read() 
                continue
            
            except urllib2.URLError as e:
                    #If any element of the signing, payload creation, or connection throws an exception then terminate since we cannot continue.
                print "URL ERROR"
                print e
                continue

    print 'Number of successfully requested gift cards:', NsuccessfullCards 
    #---------------------------------------------------------------------------------------
    #----------------------------   Send text with EZtexting -------------------------------
    #---------------------------------------------------------------------------------------
    
    cur.execute('''SELECT id, number, redeem_code, language, amount FROM Respondent 
                    WHERE (sentstatus IS NULL or sentstatus==0) 
                            and redeem_code IS NOT NULL''')

    textcontacts = cur.fetchall()

    # Count successfully sent texts
    nsuccessfull=0 
   
    if len(textcontacts)>0: # Only new respondents

        for contact in textcontacts:

            sqlDB_id = contact[0]
            number = contact[1]
            code = contact[2]
            language = contact[3]
            amount = contact[4]
        
            message = createEzMessage(code=code, amount=amount, language=language) 

            numberstr = "&PhoneNumbers[]=" + str(number)[1:11] # Do not use 1 of 001. Will not be excepted by EZtexting API
        
            # Set parameters for texting request
            params = ""
            params+= EZUserPassword
            params+= message
            params+= numberstr
            
            # Send text
            curl.setopt(pycurl.POSTFIELDS, params)
            curl.setopt(pycurl.URL, "https://app.eztexting.com/sending/messages?format=xml")
            contents = StringIO.StringIO()
            curl.setopt(pycurl.WRITEFUNCTION, contents.write) 
            curl.perform()
            
            responseCode = curl.getinfo(pycurl.HTTP_CODE)
            isSuccesResponse = responseCode < 400
            nsuccessfull = nsuccessfull + isSuccesResponse

            def getText(x): return x.text
 
            doc = ElementTree.XML(contents.getvalue())

            if (isSuccesResponse == 0):
                print 'Errors: ' + ', '.join(map(getText, doc.findall('Errors/Error')))
        
            cur.execute(''' UPDATE Respondent 
                            SET sentstatus = ?
                            WHERE id == ?''',
                            (isSuccesResponse, sqlDB_id))

            if (isSuccesResponse): # record time only if sending was successfull
                cur.execute(''' UPDATE Respondent 
                            SET sent_date=datetime(CURRENT_TIMESTAMP, 'localtime') 
                            WHERE id == ?''',
                            (sqlDB_id,))
            
            sqlconn.commit()    

    print 'Number of successfully sent texts with EZtexting:',  nsuccessfull


    #---------------------------------------------------------------------------------------
    #--------------------------   save back up copy every backupn's time  -----------------
    #---------------------------------------------------------------------------------------
    
    if t % backupn == 0:
        """Create timestamped database copy"""
        createBackup(SQLBackupPath, database)

    time.sleep(waittime)

# close sql connection
sqlconn.close()


