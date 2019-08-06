# botcannon-code
=======================
A framework for creating powerful chatbot services backed by Redis streams.


* runner - Long running process that collects/produces messages for the worker to call. Message are inserted into a stream dedicated to the bot.  
* worker - Acts on the message produced by the runner. Currently messages are process with modified `fire` code. 
* stream - key values data pairs indexed with a timestamp.  
* consumer group - A ledger of activity against a stream.