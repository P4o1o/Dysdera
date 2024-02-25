# Dysdera Web Crawler
### this is a simple asynchronous web crawler implementation.

#### it uses:
 
  MongoDB for saving data (or in alternative a Json file for testing)
  
#### and some python packages:
  
  * motor to interact with MongoDB,
  * json and aiofiles for saving in Json file
  * asyncio for the asynchronous logic,
  * aiohttp for http managing,
  * lxml for the html parsing,
  * brotli for http response compression,
  * pytz and python-dateutil for more precise datetime management,
  * chardet for encodin detection
#


#### files structure:

+ dysderacrawler.py contains the logic of the crawler, 

+ extractors.py contains the logic of the extractors, 

+ policy.py contains the structure of the crawler policy, 

+ selectionpolicy.py some selection policy

+ web.py the logic for manage webpages and more, 

+ parser.py the necessary parser 

+ logger.py the logic for the logs
