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
class JestUpdateRequestBuilder implements JestRequest<Update>{
    private final String index;
    private String type;
    private String id;
    private String script;
    private Map<String, Object> scriptParams;
    private boolean refresh;
    private IndexRequest indexRequest;
    private IndexRequest upsertRequest;

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
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     */
    public JestUpdateRequestBuilder setScript(String script) {
        this.script = script;
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
     * Should a refresh be executed post this update operation causing the operation to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public JestUpdateRequestBuilder setRefresh(boolean refresh) {
        this.refresh = refresh;
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

    public Update getAction() {
        final ObjectNode jsonNodes = Json.newObject();
        jsonNodes.put("script", script);
        jsonNodes.put("params", Json.toJson(scriptParams));
        return new Update.Builder(jsonNodes.toString()).id(id).index(index).type(type).refresh(refresh).build();
    }
}
