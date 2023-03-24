# Tests

We have split the test suite into two sections
- unittests: low level tests
- integration: tests that also connect to various apis

To run the intergration tests you will need to set the following variables
 - SS_URL: Sheffield solar URL
 - SS_USER_ID: user id for sheffield solar
 - SS_KEY: the api key for sheffield solar
 - API_KEY: Key for pvoutput.org
 - SYSTEM_ID: system id for pvoutput.org
 - DATA_SERVICE_URL: the URL for the pvoutput.org data service

TODO we should mock some of the APIs to and then more unittests can be run with out the real connection details.
