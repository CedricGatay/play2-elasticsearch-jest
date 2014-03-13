package com.github.cleverage.elasticsearch;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.searchbox.client.JestResult;
import io.searchbox.core.Get;
import io.searchbox.core.Update;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import play.libs.Json;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * @author cgatay
 */
class JestUpdateRequestBuilder {
    private final String index;
    private String type;
    private String id;
    private String routing;
    private String parent;
    private String script;
    private String scriptLang;
    private Map<String, Object> scriptParams;
    private String[] fields;
    private int retryOnConflict;
    private boolean refresh;
    private ReplicationType replicationType;
    private WriteConsistencyLevel consistencyLevel;
    private String percolate;
    private IndexRequest indexRequest;
    private Map source;
    private IndexRequest upsertRequest;
    private boolean shouldUpsertDoc;

    public JestUpdateRequestBuilder(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    /**
     * Sets the type of the indexed document.
     */
    public JestUpdateRequestBuilder setType(String type) {
        this.type = type;
        return this;
    }

    /**
     * Sets the id of the indexed document.
     */
    public JestUpdateRequestBuilder setId(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public JestUpdateRequestBuilder setRouting(String routing) {
        this.routing = routing;
        return this;
    }

    public JestUpdateRequestBuilder setParent(String parent) {
        this.parent = parent;
        return this;
    }

    /**
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     */
    public JestUpdateRequestBuilder setScript(String script) {
        this.script = script;
        return this;
    }

    /**
     * The language of the script to execute.
     */
    public JestUpdateRequestBuilder setScriptLang(String scriptLang) {
        this.scriptLang = scriptLang;
        return this;
    }

    /**
     * Sets the script parameters to use with the script.
     */
    public JestUpdateRequestBuilder setScriptParams(Map<String, Object> scriptParams) {
        this.scriptParams = scriptParams;
        return this;
    }

    /**
     * Add a script parameter.
     */
    public JestUpdateRequestBuilder addScriptParam(String name, Object value) {
        //TODO NPE
        this.scriptParams.put(name, value);
        return this;
    }

    /**
     * Explicitly specify the fields that will be returned. By default, nothing is returned.
     */
    public JestUpdateRequestBuilder setFields(String... fields) {
        this.fields = fields;
        return this;
    }

    /**
     * Sets the number of retries of a version conflict occurs because the document was updated between
     * getting it and updating it. Defaults to 0.
     */
    public JestUpdateRequestBuilder setRetryOnConflict(int retryOnConflict) {
        this.retryOnConflict = retryOnConflict;
        return this;
    }

    /**
     * Should a refresh be executed post this update operation causing the operation to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public JestUpdateRequestBuilder setRefresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    /**
     * Sets the replication type.
     */
    public JestUpdateRequestBuilder setReplicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
        return this;
    }

    /**
     * Sets the consistency level of write. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    public JestUpdateRequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    /**
     * Causes the updated document to be percolated. The parameter is the percolate query
     * to use to reduce the percolated queries that are going to run against this doc. Can be
     * set to <tt>*</tt> to indicate that all percolate queries should be run.
     */
    public JestUpdateRequestBuilder setPercolate(String percolate) {
        this.percolate = percolate;
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public JestUpdateRequestBuilder setDoc(IndexRequest indexRequest) {
        this.indexRequest = indexRequest;
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public JestUpdateRequestBuilder setDoc(XContentBuilder source) {
        this.indexRequest.source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public JestUpdateRequestBuilder setDoc(Map source) {
        this.indexRequest.source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public JestUpdateRequestBuilder setDoc(Map source, XContentType contentType) {
        this.indexRequest.source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public JestUpdateRequestBuilder setDoc(String source) {
        this.indexRequest.source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public JestUpdateRequestBuilder setDoc(byte[] source) {
        this.indexRequest.source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public JestUpdateRequestBuilder setDoc(byte[] source, int offset, int length) {
        this.indexRequest.source(source, offset, length);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public JestUpdateRequestBuilder setDoc(String field, Object value) {
        this.indexRequest.source(field, value);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified, the doc provided
     * is a field and value pairs.
     */
    public JestUpdateRequestBuilder setDoc(Object... source) {
        this.indexRequest.source(source);
        return this;
    }

    /**
     * Sets the index request to be used if the document does not exists. Otherwise, a {@link org.elasticsearch.index.engine.DocumentMissingException}
     * is thrown.
     */
    public JestUpdateRequestBuilder setUpsert(IndexRequest upsertRequest) {
        this.upsertRequest = upsertRequest;
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public JestUpdateRequestBuilder setUpsert(XContentBuilder source) {
        this.upsertRequest.source(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public JestUpdateRequestBuilder setUpsert(Map source) {
        this.upsertRequest.source(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public JestUpdateRequestBuilder setUpsert(Map source, XContentType contentType) {
        this.upsertRequest.source(source, contentType);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public JestUpdateRequestBuilder setUpsert(String source) {
        this.upsertRequest.source(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public JestUpdateRequestBuilder setUpsert(byte[] source) {
        this.upsertRequest.source(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public JestUpdateRequestBuilder setUpsert(byte[] source, int offset, int length) {
        this.upsertRequest.source(source, offset, length);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists. The doc
     * includes field and value pairs.
     */
    public JestUpdateRequestBuilder setSource(Map source) {
        this.source = source;
        return this;
    }

    /**
     * Sets whether the specified doc parameter should be used as upsert document.
     */
    public JestUpdateRequestBuilder setDocAsUpsert(boolean shouldUpsertDoc) {
        this.shouldUpsertDoc = shouldUpsertDoc;
        return this;
    }
    

    @Nullable
    public JestResult jestXcute(){
        final Update build = getAction();

        try {
            return IndexClient.client.execute(build);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Update getAction() {
        final ObjectNode jsonNodes = Json.newObject();
        jsonNodes.put("script", script);
        jsonNodes.put("params", Json.toJson(scriptParams));
        return new Update.Builder(jsonNodes.toString()).id(id).index(index).type(type).refresh(refresh).build();
    }
}
