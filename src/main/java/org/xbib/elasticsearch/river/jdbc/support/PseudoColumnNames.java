
package org.xbib.elasticsearch.river.jdbc.support;

/**
 * The names of the column names with a special meaning for the river execution.
 *
 * Mostly, they map to the Elasticsearch bulk item.
 *
 * The _job column denotes
 * an ID for the event of a fetch execution.
 */
public interface PseudoColumnNames {

    // bulk job name
    String JOB = "_job";

    // bulk operation type
    String OPTYPE = "_optype";

    // bulk document
    String INDEX = "_index";
    String TYPE = "_type";
    String ID = "_id";

    // bulk parameters
    String VERSION = "_version";
    String ROUTING = "_routing";
    String PERCOLATE = "_percolate";
    String PARENT = "_parent";
    String TIMESTAMP = "_timestamp";
    String TTL = "_ttl";

    // JSON
    String SOURCE = "_source";
}
