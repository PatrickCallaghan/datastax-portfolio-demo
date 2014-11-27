Portfolio Demo
====================

NOTE - this example requires apache cassandra version > 2.0 and the cassandra-driver-core version > 2.0.0

## Scenario
This demo shows how to save a large number trades that make up a portfolio. The process involves breaking down 
the portfolio into sizable chunks to save approx 10,000 trades per row.  

## Schema Setup
Note : This will drop the keyspace "datastax_portfolio_demo" and create a new one. All existing data will be lost. 

To specify contact points use the contactPoints command line parameter e.g. '-DcontactPoints=192.168.25.100,192.168.25.101'
The contact points can take mulitple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"

To start the demo, run

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.portfolio.Main" 
    
	
To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
    
    
