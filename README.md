# BenchFoundry
BenchFoundry is a natively distributed benchmarking framework for the execution of arbitrary application-driven cloud database benchmarks against OLTP databases. BenchFoundry logs detailed results which can be used to determine arbitrary measured qualities and to identify even minor instances of unusual behavior. For repeatability reasons, BenchFoundry executes workload traces instead of generating them on the fly.

## Current implementation state:
BenchFoundry currently supports relational database systems and right now only comes with a connector for MariaDB. At the moment, we're working on adding support for column stores and key-values stores but will also add connectors for other RDBMS.
The analysis module currently only calculates performance metrics. Right now, we're working on adding consistency metrics.
BenchFoundry already comes with a trace generator which aims to provoke the maximum staleness observable in a storage system. We'll add another trace generator based on TPC-C soon.



## Build process
1. Install Apache Thrift from [https://thrift.apache.org/download|https://thrift.apache.org/download]
2. Copy teams.properties.example to teams.properties
3. Edit `teams.properties` to set `thrift.executable` to the Thrift executable path
4. Now build and test with 
`mvn clean generate-sources compile test`
Hint: currently, some unit tests are broken - make sure to skip tests for builds :)

## Running BenchFoundry
1. Install Java on master and slave machines, put the log4j2.xml file and the benchfoundry.jar in the same folder on all machines.
2. Put an updated version of benchfoundry.properties and slaves.properties on the master machine
3. Use a trace generator or existing input files (traces, schema, oplist, param lists) and put them on the master machine
4. Start BenchFoundry on the slave machines using `java -jar benchfoundry.jar <port>` where port is identical to the one specified in the slaves.properties file
5. Start BenchFoundry on the master machine using `java -jar benchfoundry.jar <config file>` where config file specifies the location of the benchfoundry.properties file. If you omit this parameter, the BenchFoundry master will default to benchfoundry.properties in the current folder.
6. Collect the result files from the specified result directory and run an analytics process.
