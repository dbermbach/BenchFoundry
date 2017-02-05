# BenchFoundry
BenchFoundry is a natively distributed benchmarking framework for the execution of arbitrary application-driven cloud database benchmarks against OLTP databases. BenchFoundry logs detailed results which can be used to determine arbitrary measured qualities and to identify even minor instances of unusual behavior. For repeatability reasons, BenchFoundry executes workload traces instead of generating them on the fly.

## Current implementation state:
BenchFoundry currently supports relational database systems and right now only comes with a connector for MariaDB. At the moment, we're working on adding support for column stores and key-values stores but will also add connectors for other RDBMS.
The analysis module currently only calculates performance metrics. Right now, we're working on adding consistency metrics.
BenchFoundry already comes with a trace generator which aims to provoke the maximum staleness observable in a storage system and another trace generator based on TPC-C.



## Build process
1. Install Apache Thrift from [https://thrift.apache.org/download]
2. Copy teams.properties.example to teams.properties
3. Edit `teams.properties` to set `thrift.executable` to the Thrift executable path
4. Now build and test with 
`mvn clean generate-sources compile test assembly:single`
Hint: currently, some unit tests may be broken - make sure to skip tests for builds :)

## Running BenchFoundry
1. Install Java on master and slave machines, put the log4j2.xml file and the benchfoundry.jar in the same folder on all machines.
2. Put an updated version of benchfoundry.properties and slaves.properties on the master machine
3. Use a trace generator or existing input files (traces, schema, oplist, param lists) and put them on the master machine
4. Start BenchFoundry on the slave machines using `java -jar BenchFoundry-1.0-SNAPSHOT-jar-with-dependencies.jar <port>` where port is identical to the one specified in the slaves.properties file
5. Start BenchFoundry on the master machine using `java -jar BenchFoundry-1.0-SNAPSHOT-jar-with-dependencies.jar <config file>` where config file specifies the location of the benchfoundry.properties file. If you omit this parameter, the BenchFoundry master will default to benchfoundry.properties in the current folder.
6. Collect the result files from the specified result directory and run an analytics process.

## Workload Generators
### Consistency Benchmark
Simply run the main class de.tuberlin.ise.benchfoundry.tracegeneration.consistencybenchmark.ConsistencyBenchmarkingTraceGenerator which will interactively query input parameters and create the output trace file. Additional parameters are (somewhat hidden) in class de.tuberlin.ise.benchfoundry.tracegeneration.consistencybenchmark.StaticContent

### TPC-C Inspired Benchmark
#### Main Class
de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark.TraceGenerator.java

#### Generator Outputs (8 files)
1. **tpcc_props**: Contains main properties for the execution of the benchmark.
2. **tpcc_schema**: Contains schema definitions.
3. **tpcc_operation**: Contains business operations used in PRELOAD, WARMUP, or EXPERIMENT phases.
4. **tpcc_param**: Contains parameters used by business operations in PRELOAD, WARMUP, or EXPERIMENT phases.
5. **tpcc_cparam**: Contains additional custom parameters used by business operations in PRELOAD, WARMUP, or EXPERIMENT phases.
6. **tpcc_load**: Contains business processes for the phase PRELOAD.
7. **tpcc_warm**: Contains business processes for the phase WARMUP.
8. **tpcc_run**: Contains business processes for the phase EXPERIMENT.

#### CLI parameters (6 options)
1. "**--datasetScaler**" [Integer i: i>0 (default: 1)] - Scales the inital dataset for the benchmark according to the TPC-C specification (#WAREHOUSES).
2. "**--runtime**" [Integer i: i>0 (default: 120)] - Defines the runtime of the RUN phase in seconds.
3. "**--paymentProcessTarget**" [Integer i: i>-1 (default: 1)] - Defines the target number of "PAYMENT" processes that are scheduled per second.
4. "**--orderstatusProcessTarget**" [Integer i: i>-1 (default: 1)] - Defines the target number of "ORDERSTATUS" processes that are scheduled per second.
5. "**--neworderProcessTarget**" [Integer i: i>-1 (default: 1)] - Defines the target number of "NEWORDER" processes that are scheduled per second.
6. "**--processTargetScaler**" [Integer i: i>-1 (default: 10)] - Scales (multiplies) the target number of all processes that are scheduled per second.


