/**
 * 
 */
package de.tuberlin.ise.benchfoundry.physicalschema.factory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tuberlin.ise.benchfoundry.physicalschema.model.AbstractPhysicalSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.relational.RelationalPhysicalSchema;
import de.tuberlin.ise.benchfoundry.physicalschema.model.relational.RelationalRequest;
import de.tuberlin.ise.benchfoundry.util.BenchFoundryConfigData;
import de.tuberlin.ise.benchfoundry.util.QueryInputFileParser;

/**
 * @author Dave
 *
 */
public class RelationalPhysicalSchemaFactory extends
		AbstractPhysicalSchemaFactory {

	private static final Logger LOG = LogManager
			.getLogger(RelationalPhysicalSchemaFactory.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.physicalschema.factory.AbstractPhysicalSchemaFactory
	 * #createPhysicalSchema()
	 */
	@Override
	public AbstractPhysicalSchema createPhysicalSchema() {
		RelationalPhysicalSchema schema = new RelationalPhysicalSchema();
		for (Entry<Integer, RelationalRequest> entry : parseQueryInputFile(
				BenchFoundryConfigData.oplistFilename).entrySet()) {
			schema.registerRequestsForQueryId(entry.getKey(),
					Arrays.asList(entry.getValue()));
		}
		return schema;
	}

	/**
	 * parses the specified query input file and creates a
	 * {@link RelationalRequest} instance for each entry.
	 * 
	 * @param filename
	 *            name of the query input file
	 * @return a map containing the query ID as key and the corresponding
	 *         {@link RelationalRequest} as value.
	 */
	private Map<Integer, RelationalRequest> parseQueryInputFile(String filename) {
		Map<Integer, RelationalRequest> result = new HashMap<>();
		try {
			for (Entry<Integer, String> entry : QueryInputFileParser
					.readQueryInputFileFromStream(new FileInputStream(filename))
					.entrySet()) {
				result.put(entry.getKey(),
						new RelationalRequest(entry.getValue(), entry.getKey()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.physicalschema.factory.AbstractPhysicalSchemaFactory
	 * #serializeSchema(de.tuberlin.ise.benchfoundry.physicalschema.model.
	 * AbstractPhysicalSchema)
	 */
	@Override
	public byte[] serializeSchema(AbstractPhysicalSchema schema) {
		if (!(schema instanceof RelationalPhysicalSchema))
			return null;
		RelationalPhysicalSchema rps = (RelationalPhysicalSchema) schema;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeInt(rps.getIdSet().size());
			for (Integer i : rps.getIdSet()) {

				oos.writeInt(i); // id
				oos.writeObject(((RelationalRequest) (rps
						.getRequestsForQueryId(i).get(0))).getSqlQuery()); // sql
																			// string

			}

			oos.close();
		} catch (Exception e) {
			LOG.error("Error while serializing schema: " + e.getMessage(), e);
			e.printStackTrace();
			return null;
		}
		return baos.toByteArray();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.ise.benchfoundry.physicalschema.factory.AbstractPhysicalSchemaFactory
	 * #deserializeSchema(byte[])
	 */
	@Override
	public AbstractPhysicalSchema deserializeSchema(byte[] serializedSchema) {
		List<RelationalRequest> list;
		RelationalPhysicalSchema schema = new RelationalPhysicalSchema();
		try {
			ObjectInputStream ois = new ObjectInputStream(
					new ByteArrayInputStream(serializedSchema));
			int number = ois.readInt();
			int counter = 0;
			int id;
			RelationalRequest req;
			while (counter++ < number) {
				id = ois.readInt();
				req = new RelationalRequest((String) (ois.readObject()), id);
				list = new ArrayList<RelationalRequest>();
				list.add(req);
				schema.registerRequestsForQueryId((int) id, list);
			}
			ois.close();
		} catch (Exception e) {
			LOG.error("Error while deserializing schema: " + e.getMessage(), e);
			e.printStackTrace();
			return null;
		}
		return schema;
	}
}
