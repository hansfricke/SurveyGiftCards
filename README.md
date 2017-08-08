# SurveyGiftCards

This library contains several python scripts to communicate witht the Qualtrics API to facilitate sending gift card codes to respondents, and creating contacts lists for distributions. For all scripts, SQLite needs to be installed. See scripts for the parameters that need to be specified by the user. 

I am not a professional python developer. If you see ways how to improve the program, please let me know. I am always happy to learn new things. I also will not take any responsibility for the proper functioning of the program. If you decide to use it, it is your responsibility to adapt it to your application and thoroughly test it I used python 2.7.11 (64-bit). 


## sendGiftCards

This program sends out gift card codes to survey respondents
1. Downloads new survey responses from  Qaultrics API 
2. Writes the new contact info into SQLite database
3. Gets a gift card code from .csv file for each new respondent and adds it to data base
4. Sends the gift card codes to new repsondents via email
5. repeat 


## sendGiftCardsAmazon

This program sends out gift card codes to survey respondents
1. Downloads new survey responses from  Qualtrics API 
2. Writes the new contact info into SQLite database
3. Request a gift card code from Amazon Incentives API for each new respondent and adds it to data base (This code is for the sandbox environment, for productoin change host and serviceTarget)
4. Sends the gift card codes to new repsondents via EZtexting.com API 
5. repeat 

## cancelGiftCardsAmazon

This program cancels gift card codes over the Amazon Icentives API for codes that have already been assigned
1. Fetches the gift card id and request id from the SQL database
2. Cancels the the gift card over the Amazon Incentives API
3. Records the cancelation date in the SQL database 


## createContactLists

This program updates contact lists for survey distribution and uploads them to Qualtrics. This is useful if you do not always use the same mode of distributing the survey (e.g Email and TEXT or if you want to use text reminders

1. Fetches the IDs of respondents from SQL database
2. Reads in original contact lists, filters out respondents, and saves updated contact lists
3. Creates new mailing lists in Qualtrics
4. Adds contacts to new mailing lists 

