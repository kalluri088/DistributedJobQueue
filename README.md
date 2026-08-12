A lightweight distributed job queue implemented in Python that demonstrates a client–dispatcher–worker architecture for processing text files. 
The client.py submits text-processing jobs, dispatcher.py queues and assigns jobs to
available workers, and worker.py executes tasks (word count, lowercase conversion, top-word frequency) and returns results.

Architecture: Client → Dispatcher (queue + scheduler) → Worker(s)
Core tasks: word count, lowercase conversion, top-5 word frequency
Tech: Python sockets, length-prefixed JSON messages, threading, optional SSL for worker connections
