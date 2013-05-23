package com.inmobi.grill.elasticsearch;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import static com.google.common.base.Preconditions.*;

public class ElasticSearchIndexer {
	public static final String ES_DEFAULT_CLUSTER = "elasticsearch";
	public static final int ES_DEFAULT_PORT = 9300;

	Logger LOG = Logger.getLogger(ElasticSearchIndexer.class.getName());

	Client client;
	private String cluster;
	private String host;
	private int port;

	private static final String PUT_MULTI_TYPES_TEMPLATE = "{\n" + "    \""
			+ Document.EVENT_TYPE + "\" : {\n" + "\"_all\" : {\n"
			+ "\"enabled\": false" + "    },\n" + "\"_source\" : {\n"
			+ "\"enabled\":false" + "    },\n" + "        \"properties\" : {\n"
			+ "%s" + "        }\n" + "    }\n" + "}";

	private static final String TYPE_TEMPLATE = "\"%s\" : {\"type\" : \"%s\"}";
	private static final String TYPE_TEMPLATE_STRING = "\"%s\" : {\"type\" : \"%s\" ,\"store\" : \"no\",	\"index\" : \"not_analyzed\" ,\"omit_norms\": true , \"omit_term_freq_and_positions\": true}";
	private static final String TYPE_TEMPLATE_NUMERIC = "\"%s\" : {\"type\" : \"%s\" ,\"store\" : \"yes\",	\"precision_step\": 64}";

	/**
	 * Close connection to elastic search cluster
	 */
	public void close() {
		if (client != null) {
			try {
				client.close();
			} catch (Exception exc) {
				// LOG.error("Error closing elasticsearch connection", exc);
			}
		}
	}

	/**
	 * Connects to the ElasticSearch cluster. This should be called before
	 * attempting index operation, except when a ElasticSearch client was
	 * explicitly set using setClient.
	 * <p>
	 * This uses a ElasticSearch <a href=
	 * "http://www.elasticsearch.org/guide/reference/java-api/client.html"
	 * >TransportClient</a>
	 * </p>
	 * 
	 * @throws IOException
	 */
	public void connect() throws IOException {
		try {
			Settings settings = ImmutableSettings.settingsBuilder()
					.put("cluster.name", cluster)
					.put("client.transport.sniff", true).build();
			TransportClient tc = new TransportClient(settings);
			tc.addTransportAddress(new InetSocketTransportAddress(host, port));
			this.client = tc;
		} catch (ElasticSearchException exc) {
			throw new IOException(exc);
		}
	}

	public ElasticSearchIndexer(String cluster, String host, int port) {
		checkNotNull(cluster);
		checkNotNull(host);
		checkNotNull(port);
		this.cluster = cluster;
		this.host = host;
		this.port = port;
	}

	/**
	 * Create indexer with default clustername and default port
	 * 
	 * @param host
	 * @throws IOException
	 */
	public ElasticSearchIndexer(String host) {
		this(ES_DEFAULT_CLUSTER, host, ES_DEFAULT_PORT);
	}

	/**
	 * Create indexer with default port
	 */
	public ElasticSearchIndexer(String cluster, String host) {
		this(cluster, host, ES_DEFAULT_PORT);
	}

	public ElasticSearchIndexer() {

	}

	protected void setClient(Client client) {
		this.client = client;
	}

	/**
	 * Index a batch of events, with optional parameter to wait until the batch
	 * is 'refreshed' in ElasticSearch. See <a href=
	 * "http://www.elasticsearch.org/guide/reference/api/admin-indices-refresh.html"
	 * >ElasticSearch refresh API</a>
	 * 
	 * @param events
	 *            Event batch for indexing
	 * @param indexName
	 *            name of the index
	 * @param refresh
	 *            if true, the batch is made immediately searchable
	 * @param overwrite
	 *            event if event with the same id ({@link Event.getIndexId()})
	 *            is already present in the index. When false, throws an
	 *            exception.
	 * @throws IOException
	 */
	public void indexBatch(String indexName, Iterator<Document> events,
			boolean refresh, boolean overwrite) throws IOException {
		checkNotNull(indexName);
		checkNotNull(events);

		BulkRequestBuilder brb = client.prepareBulk();

		while (events.hasNext()) {
			Document evt = events.next();

			if (evt.getId() == null) {
				brb.add(client.prepareIndex(indexName, Document.EVENT_TYPE)
						.setSource(evt.getDocument())
						.setOpType(overwrite ? OpType.INDEX : OpType.CREATE));

			} else {
				brb.add(client.prepareIndex(indexName, Document.EVENT_TYPE)
						.setSource(evt.getDocument()).setId(evt.getId())
						.setOpType(overwrite ? OpType.INDEX : OpType.CREATE));
			}
		}

		BulkResponse response = brb.execute().actionGet();

		// TODO XXX Check if all requests succeeded
		if (response.hasFailures()) {
			throw new IOException(response.buildFailureMessage());
		}

		// make it searchable immediately, this sends one more request to the
		// server
		// This will affect all shards of the index
		if (refresh) {
			RefreshRequest refreshReq = new RefreshRequest();
			refreshReq.indices(indexName);
			client.admin().indices().refresh(refreshReq).actionGet();
		}
	}

	/**
	 * Set types of multiple fields in one go. Takes a map of field names to
	 * types, and the index name as arguments.
	 * <p>
	 * See <a href=
	 * "http://www.elasticsearch.org/guide/reference/mapping/core-types.html"
	 * >ElasticSearch type</a> documentation for allowed types and their
	 * meaning. This funcion allows setting only the type of the field, and not
	 * other field parameters (like stored, analyzer etc.).
	 * </p>
	 * 
	 * @param index
	 * @throws IOException
	 */
	public void setTypeMap(String index, Map<String, String> typeMap)
			throws IOException {
		checkNotNull(index);
		checkNotNull(typeMap);

		// PutMappingRequestBuilder reqBuilder =
		// client.admin().indices().preparePutMapping(index);
		// reqBuilder.setType(Event.EVENT_TYPE);

		int i = 0;
		int numTypes = typeMap.size();
		StringBuilder typeSrc = new StringBuilder();
		for (String field : typeMap.keySet()) {
			if (isNumeric(typeMap.get(field))) {
				typeSrc.append(String.format(TYPE_TEMPLATE_NUMERIC, field,
						typeMap.get(field)));
			} else if (isString(typeMap.get(field))) {
				typeSrc.append(String.format(TYPE_TEMPLATE_STRING, field,
						typeMap.get(field)));
			} else {
				typeSrc.append(String.format(TYPE_TEMPLATE, field,
						typeMap.get(field)));
			}
			if (i++ < numTypes - 1) {
				typeSrc.append(",\n");
			}
		}

		String reqSource = String.format(PUT_MULTI_TYPES_TEMPLATE,
				typeSrc.toString());
		System.out.println("Mapping" + reqSource);
		// reqBuilder.setSource(reqSource).execute().actionGet();
	}

	private boolean isNumeric(String type) {
		return type.equals("int") || type.equals("long")
				|| type.equals("float") || type.equals("double")
				|| type.equals("date");

	}

	private boolean isString(String type) {
		return type.equals("string");
	}

}
