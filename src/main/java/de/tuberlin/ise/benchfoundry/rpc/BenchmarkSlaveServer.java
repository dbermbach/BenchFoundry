/**
 * @author Akon Dey (akon.dey@gmail.com)
 *
 */
package de.tuberlin.ise.benchfoundry.rpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import de.tuberlin.ise.benchfoundry.rpc.BenchmarkSlaveService;

public class BenchmarkSlaveServer {
	private static final Logger LOG = LogManager
			.getLogger(BenchmarkSlaveServer.class);

	private BenchmarkSlaveServiceHandler handler;

	private BenchmarkSlaveService.Processor<BenchmarkSlaveServiceHandler> processor;

	/** runs the thrift server in parallel */
	private Thread handlerThread;

	/**
	 * Only invoke once per port. Subsequent invocations will crash since the
	 * port will already be in use.
	 * 
	 * @param port
	 *            port of the thrift server
	 * 
	 * @return a new running instance of the BenchmarkSlaveServer.
	 */
	public static BenchmarkSlaveServer startNewSlaveServer(int port) {
		BenchmarkSlaveServer s = new BenchmarkSlaveServer();
		s.serve(port);
		return s;
	}

	/**
	 * starts the Thrift service
	 */
	private void serve(int port) {
		try {

			this.handler = new BenchmarkSlaveServiceHandler();
			this.processor = new BenchmarkSlaveService.Processor<BenchmarkSlaveServiceHandler>(
					handler);

			this.handlerThread = new Thread(new Runnable() {
				public void run() {
					try {
						TServerTransport serverTransport = new TServerSocket(
								port);
						TServer server = new TSimpleServer(new Args(
								serverTransport).processor(processor));
						server.serve();
						LOG.info("Exiting the benchmark slave server...");
					} catch (Exception e) {
						LOG.error(
								"Exception encountered, exiting benchmark slave server: "
										+ e.getMessage(), e);
						e.printStackTrace(System.err);
					}
				}
			});

			this.handlerThread.setName("Thrift");
			this.handlerThread.setDaemon(true);
			this.handlerThread.start();

		} catch (Exception x) {
			LOG.error(
					"Failure while starting or running Thrift server: "
							+ x.getMessage(), x);
			x.printStackTrace();
		}
	}

}