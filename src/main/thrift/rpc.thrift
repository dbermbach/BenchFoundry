namespace java de.tuberlin.ise.benchfoundry.rpc
namespace py de.tuberlin.ise.benchfoundry.rpc

typedef i32 int // We can use typedef to get pretty names for the types we are using
typedef i64 long // We can use typedef to get pretty names for the types we are using


service BenchmarkSlaveService
{
	/**
	 * The hello method identifies the benchmark slave.
	 * @return a hello response identifying the slave
	 */
	string hello(),

	/**
	 * Load the configuration settings from file specified at the benchmark slave.
	 */
	void loadConfig(1:string config),

	/**
	 * Load the traces from file specified at the benchmark slave.
	 */	
	void loadTrace(1:string trace),
	
	/**
	 * Propose a start time for the phase of operation to the benchmark slave.
	 * @return the slave returns the input start timestamp if it accepts it else returns an 
	 *         acceptable timestamp to propose a new one.
	 */
	long proposeStartTimestamp(1:string phase, 2:long timestamp),

	/**
	 * Finalize a start time for the phase of operation to the benchmark slave. The slaves accept
	 * the input start timestamp else throws an exception on error.
	 */
	void commitStartTimestamp(1:string phase, 2:long timestamp),
	
	/**
	 * Abort the phase of operation specified or abort the current executing phase if an empty string
	 * is specified. This throws an excpetion on failure.
	 */
	void abortPhase(1:string phase = ""),

	/**
	 * Wait for the phase of operation specified or the current executing phase if an empty string
	 * is specified to complete. This throws an exception on failure.
	 */
	void waitPhase(1:string phase = ""),

	/**
	 * Execute a command specified at the slave. Slaves may implement named commands that use the 
	 * list of arguments passed.
	 */ 
	string executeCommand(1:string command, 2:list<string> arguments),
	
	/**
	 * Send an arbitrary BLOB of data to the client. This can be used to extend the default client-
	 * server (master-slave) communication with arbitrary messages by encoding them as BLOBs.
	 */ 
	void sendBlob(1:string fileType, 2:binary blob),
}