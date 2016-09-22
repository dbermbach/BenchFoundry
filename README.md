# Build process
1. Install Apache Thrift from [https://thrift.apache.org/download|https://thrift.apache.org/download]
2. Copy teams.properties.example to teams.properties
3. Edit `teams.properties` to set `thrift.executable` to the Thrift executable path
4. Now build and test with 
`mvn clean generate-sources compile test`
Hint: currently, some unit tests are broken - make sure to skip tests for builds :)

# Running BenchFoundry
1. Install Java on master and slave machines, put the log4j2.xml file and the benchmw.jar in the same folder on all machines.
2. Put an updated version of benchfoundry.properties and slaves.properties on the master machine
3. Use a trace generator or existing input files (traces, schema, oplist, param lists) and put them on the master machine
4. Start BenchFoundry on the slave machines using `java -jar benchfoundry.jar <port>` where port is identical to the one specified in the slaves.properties file
5. Start BenchFoundry on the master machine using `java -jar benchfoundry.jar <config file>` where config file specifies the location of the benchfoundry.properties file. If you omit this parameter, the BenchFoundry master will default to benchfoundry.properties in the current folder.
6. Collect the result files from the specified result directory and run an analytics process.
