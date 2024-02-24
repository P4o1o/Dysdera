# Dysdera Web Crawler
this is a simple asynchronous web crawler implementation.
it uses:
  MongoDB for saving data
and some python packages:
  motor to intercact with MongoDB
  asyncio for the asynchronous logic,
  aiohttp for http managing,
  lxml for the html parsing,
  brotli for http response compression,
  pytz and python-dateutil for more precise datetime management
  chardet for encodin detection

in dysdera.py there's the logic of the crawler, 

in extractors.py the logic of the extractors, 

in policy.py is defined the structure of the crawler policy, 

in selectionpolicy.py some selection policy

in web.py the logic for manage webpages and more, 

in parser.py the needed parser 

and in logger.py the logic for the logs
