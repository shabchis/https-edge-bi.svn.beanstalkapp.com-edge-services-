https://test.salesforce.com/services/oauth2/authorize?response_type=code&client_id=3MVG9GiqKapCZBwG.OqpT.DCgHmIXOlszzCpZPxbRyvzPDNlshB5LD0x94rQO5SzGOAZrWPNIPm_aGR7nBeXe&redirect_uri=http://localhost:8080/RestTest/oauth/_callback
https://login.salesforce.com/services/oauth2/authorize?response_type=code&client_id=3MVG9yZ.WNe6byQCa824BURpmLmBd4uyCY_xsDDj9jE.681uxAfte9ACgrXVls_Z9UtHonJdBj_2_rQnmr9Ja&redirect_uri=http://localhost:8080/RestTest/oauth/_callback

this is how you get the constant code (test for sandbox login for production) run the above url when you are connected to
salesforce.you will get to url contains code="the code" get "the code"
clientid=from sales force application>setup>develop>remote access>choose api application=Consumer Key
redirecturi= "" redirecturi
the code is the constantcode property of theservice




create new api
Setup--->devlop---->remote access--->new
1. apllication name
2. redirecturi http://localhost:8080/RestTest/oauth/_callback
3. contactemail.



for regular objects ------> salesforce object reference
http://www.salesforce.com/us/developer/docs/api/Content/sforce_api_objects_list.htm#topic-title


for customize objects
Setup--->create---->objects---->choose the needed object---->get the apiname of the object and fields and build salesforcequery
example:
Select Name, Edge_Tracker__c,Download_Date__c,Trail_Activation_Date__c From Trail__c WHERE (Edge_Tracker__c!=null AND Edge_Tracker__c>0) AND ((CALENDAR_YEAR(Download_Date__c)={0} AND CALENDAR_MONTH(Download_Date__c)={1} AND DAY_IN_MONTH(Download_Date__c)={2}) OR (CALENDAR_YEAR(Trail_Activation_Date__c)={0} AND CALENDAR_MONTH(Trail_Activation_Date__c)={1} AND DAY_IN_MONTH(Trail_Activation_Date__c)={2}))

for reading about saleforce query:
http://www.salesforce.com/us/developer/docs/api/Content/sforce_api_calls_soql.htm

for more reading:
http://www.salesforce.com/us/developer/docs/api/index.htm
http://danlb.blogspot.co.il/2011/02/salesforce-rest-api-query.html
http://wiki.developerforce.com/page/Getting_Started_with_the_Force.com_REST_API#Using_the_Force.com_REST_API
http://wiki.developerforce.com/page/Digging_Deeper_into_OAuth_2.0_on_Force.com#Obtaining_an_Access_Token_in_a_Web_Application_.28Web_Server_Flow.29

