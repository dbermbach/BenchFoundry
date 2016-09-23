package de.tuberlin.ise.benchfoundry.tracegeneration.tpccinspiredbenchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * This class contains a set of utility methods to write BenchMW trace files to
 * the file system. A complete BenchMW workload contains 8 files. We describe
 * each file in the context of relational database systems.
 * 
 * @author Joern Kuhlenkamp
 *
 */
public class TraceFileWriter {

	private final String benchmark;
	private final Map<String, Long> operations;
	private final Map<String, Long> params;

	public enum TraceFile {
		SCHEMA, OPERATION, PARAM, CPARAM, LOAD, WARM, RUN, PROPS
	};

	private long schemaFileLines = 0;
	private long operationFileLines = 0;
	private long paramFileLines = 0;
	private long cparamFileLines = 0;
	private long loadFileLines = 0;
	private long warmFileLines = 0;
	private long runFileLines = 0;
	private long propFileLines = 0;

	public TraceFileWriter(String benchmark) {
		this.benchmark = benchmark;
		operations = new HashMap<>();
		params = new HashMap<>();
	}

	private String getFilename(TraceFile file) {
		return benchmark + "_" + file.toString().toLowerCase();
	}

	private void createFile(TraceFile file) throws IOException {
		Path path = Paths.get("", benchmark);
		if (!Files.exists(path)) {
			try {
				Files.createDirectory(path);
			} catch (IOException e) {
				throw new IOException("Cannot create directory: " + path.toAbsolutePath().toString() + " \n" + e);
			}
		}
		path = Paths.get("", benchmark, getFilename(file));
		if (!Files.exists(path)) {
			try {
				Files.createFile(path);
				System.out.println("Creating file: " + path.toAbsolutePath());
			} catch (IOException e) {
				throw new IOException("Cannot create file." + getFilename(file) + "\n" + e);
			}
		}
	}

	public void touchFiles() throws IOException {
		createFile(TraceFile.SCHEMA);
		createFile(TraceFile.OPERATION);
		createFile(TraceFile.PARAM);
		createFile(TraceFile.CPARAM);
		createFile(TraceFile.LOAD);
		createFile(TraceFile.WARM);
		createFile(TraceFile.RUN);
		createFile(TraceFile.PROPS);
	}

	private void appendEntry(TraceFile file, String content) throws IOException {
		switch (file) {
		case SCHEMA:

			break;

		default:
			break;
		}
		Path path = Paths.get("", benchmark, getFilename(file));
		try {
			Files.write(path, (content).getBytes(), StandardOpenOption.APPEND);
		} catch (IOException e) {
			throw new IOException("Cannot append line " + content + " to file." + getFilename(file) + "\n" + e);
		}
	}

	/**
	 * Appends an entry to the specified trace file.
	 * @param file
	 * @param content
	 * @return Id of the corresponding line or -1 for not specified
	 * @throws IOException
	 */
	public long append(TraceFile file, String content) throws IOException {
		switch (file) {
		case SCHEMA:
			appendEntry(file, schemaFileLines + ":" + content + "\n");
			return schemaFileLines++;
		case OPERATION:
			if(operations.containsKey(content))
				return operations.get(content);
			operations.put(content, operationFileLines);
			appendEntry(file, operationFileLines + ":" + content + "\n");
			return operationFileLines++;
		case PARAM:
			if(params.containsKey(content))
				return params.get(content);
			params.put(content, paramFileLines);
			appendEntry(file, paramFileLines + ":" + content + "\n");
			return paramFileLines++;
		case CPARAM:
			appendEntry(file, cparamFileLines + ":" + content + "\n");
			return cparamFileLines++;
		case LOAD:
			appendEntry(file, content + "\n");
			return loadFileLines++;
		case WARM:
			appendEntry(file, content + "\n");
			warmFileLines++;
			return warmFileLines;
		case RUN:
			appendEntry(file, content + "\n");
			return runFileLines++;
		case PROPS:
			appendEntry(file, content + "\n");
			return propFileLines++;
		default:
			return -1;
		}
	}
}
