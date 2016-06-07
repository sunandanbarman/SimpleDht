# SimpleDht

This assignment was done as academic requirement of CSE586 under Prof Steve Ko of SUNY Buffalo.

Simple DHT based on Chord.

In this assignment, design a simple DHT based on Chord. Although the design is based on Chord, it is a simplified version 
of Chord; you do not need to implement finger tables and finger­based routing; you also do not need to handle node leaves/failures.Therefore, there are three things you need to implement:

1. ID space partitioning/re­partitioning
2. Ring­based routing
3. Node joins. 


The content provider should implement all DHT functionalities and support insert and query operations. Thus, if you 
run multiple instances of your app, all content provider instances should form a Chord ring and serve insert/query 
requests in a distributed fashion according to the Chord protocol.

See PA3Specification.pdf for full specification details.
