# Hans Fricke 
# 02/13/2017
# python 2.7.11 (64-bit)

# Program to cancels gift card codes over the Amazon Icentives API
# 1) Fetches the gift card id and request id from the SQL database
# 2) Cancels the the gift card over the Amazon Incentives API (for sanbox environment)
# 3) Records the cancelation date in the SQL database 


# In this code you need to specify:
'''
- path
- SurveyExportPath 
- SQLDatabasePath
- SQLBackupPath 
- SQLDatabaseName 
- either codelist or pseudoIDlist
- awsKeyID 
- awsSecretKey 
- partnerID
'''

import os
import sqlite3
import binascii
import datetime
import hmac, hashlib
import json
import urllib2
import xml.etree.ElementTree as ElementTree


#---------------------------------------------------------------------------------------
#------------------------- Specify paths and parameters --------------------------------
#---------------------------------------------------------------------------------------

# Set path of the main folder 
path= 'D:/YOUR PROJECT FOLDER'
os.chdir(path)

# Setting path for dataexport and storage
SurveyExportPath = path + '/DownloadFolder'
SQLDatabasePath = path + '/DataBase/'
SQLBackupPath = path + '/DataBase/Archive/'
SQLDatabaseName = 'ResponseTracker.sqlite'

#---------------------------------------------------------------------------------------
#----------------------  Define cards to be canceled  ----------------------------------
#---------------------------------------------------------------------------------------

# Chose either or
# Example codelist: ['G7VG-QJVGF8-7K84', 'MB8R-LVEV38-QEEY']
# Example pseudoIDli: [1,2,3]
codelist = []
pseudoIDlist =[]           

#---------------------------------------------------------------------------------------
#----------------------- Set up Amazon API connection ----------------------------------
#---------------------------------------------------------------------------------------

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
    #serviceOperation = AGCODServiceOperation.CreateGiftCard
    serviceOperation = AGCODServiceOperation.CancelGiftCard
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
#----------------------------- Add column to SQL database ------------------------------
#---------------------------------------------------------------------------------------

# general architecture

database = SQLDatabasePath+SQLDatabaseName
sqlconn = sqlite3.connect(database)
cur = sqlconn.cursor()

try:
    cur.executescript('''
        ALTER TABLE Respondent ADD COLUMN cancel_date DATETIME
    ''')
except sqlconn.OperationalError:
    print "Column cancel_date already exists"

sqlconn.commit() 

#---------------------------------------------------------------------------------------
#------------ Querying the information for the cancel gift card API --------------------
#---------------------------------------------------------------------------------------

# Decide if codes or IDs are used to cancel cards

useCodes = False
if len(codelist) > 0:
    useCodes = True
    relavantList = codelist

elif len(pseudoIDlist) > 0: 
    relavantList = pseudoIDlist       

# Query the relevant information from SQL database
CancelCards=[]
for item in relavantList:
    if useCodes:
        cur.execute('''SELECT request_id, giftcard_id FROM Respondent 
                    WHERE redeem_code == ? and cancel_date IS NULL''',
                   (item,))
    else:    
        cur.execute('''SELECT request_id, giftcard_id FROM Respondent 
                        WHERE pseudo_id == ? and cancel_date IS NULL''',
                   (item,))
    carddata= cur.fetchone()
    if carddata != None:
        CancelCards.append(carddata)


#---------------------------------------------------------------------------------------
#-------------------------------- Cancel the gift cards --------------------------------
#---------------------------------------------------------------------------------------

if len(CancelCards)>0:           
    for card in CancelCards:

        # Extract data
        request_id = card[0]
        gcId = card[1]    
                         
        app = AppConstants()
        app.requestID = request_id 
        app.gcId = gcId
        app.dateTimeString = datetime.datetime.utcnow().strftime(app.DATE_FORMAT)
    
        # Initialize whole payload in the specified format for the given operation and set additional headers based on these settings
        payload, contentType = setPayload()
            
        # Create the URL connection to the host
        hostConnection = app.hostName
        conn = urllib2.Request(url=hostConnection)

        # Create Authentication signature and set the connection parameters
        signRequestAWSv4(conn, payload, contentType)            
        try:


            # Write the output message to the connection, if it errors it will generate an IOException
            conn.add_data(payload)
            outdata = conn.get_data() 
            
            req = urllib2.urlopen(conn)
            response = req.read()
            print response
            req.close()
            
            responseContent = ElementTree.fromstring(response)
            status = responseContent.findtext('.//status')
            
            # Record in SQL database that gift card code has been canceled 
            if status == 'SUCCESS':
                cur.execute(''' UPDATE Respondent 
                            SET cancel_date = datetime(CURRENT_TIMESTAMP, 'localtime') 
                            WHERE request_id == ?''',
                            (request_id,))
            
                sqlconn.commit()  
        
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


