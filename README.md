# Dysdera Web Crawler
### asynchronous web crawler implementation written in Python
The Dysdera Web Crawler is a Python-based asynchronous web crawler designed for maximum extensibility and adaptability.
Engineered to handle the often non-standard nature of the web, it provides fine-grained control over crawling policies.

This is not a finished project, feel free to collaborate
#



### Dependencies:
  - [**Python**](https://www.python.org/downloads/) version >= 3.9
  - [**MongoDB**](https://www.mongodb.com/) for saving data (Community edition is free) or in alternative a **Json file**
  - **some python packages:** to be installed with [pip](https://pypi.org/project/pip/) with the comand: 'pip install -r requirements.txt'
      - **motor** to interact with MongoDB,
      - **json** and **aiofiles** for saving in Json file
      - **asyncio** for the asynchronous logic,
      - **aiohttp** for http managing,
      - **lxml** for the html parsing,
      - **brotli** for http response compression,
      - **pytz** and **python-dateutil** for more precise datetime management,
      - **chardet** for encodin detection
#


### How to use it?
You will find the documentation [here](https://p4o1o.github.io/Dysdera/dysderacrawler.html)

#


### Disclamer
This is not a finished project. If you have something to add, do it!

#


### files structure:

+ **dysderacrawler.py** contains the logic of the crawler, 

+ **extractors.py** contains the logic of the extractors, 

+ **policy.py** contains the structure of the crawler policy, 

+ **selectionpolicy.py** some selection policy

+ **web.py** the logic for manage webpages and more, 

+ **parser.py** the necessary parser 

+ **logger.py** the logic for the logs

#

