package com.github.cleverage.elasticsearch;

import io.searchbox.client.JestResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Get;
import org.elasticsearch.index.VersionType;

import javax.annotation.Nullable;

/**
 * @author cgatay
 */
class JestDeleteRequestBuilder {
    private final String index;
    private String type;
    private String id;
    @Nullable
    private String routing;
    private boolean refresh;
    private long version;
    private VersionType versionType = VersionType.INTERNAL;

    public JestDeleteRequestBuilder(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    /**
     * The type of the document to delete.
     */
    public String type() {
        return type;
    }

    /**
     * Sets the type of the document to delete.
     */
    public JestDeleteRequestBuilder type(String type) {
        this.type = type;
        return this;
    }

    /**
     * The id of the document to delete.
     */
    public String id() {
        return id;
    }

    /**
     * Sets the id of the document to delete.
     */
    public JestDeleteRequestBuilder id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Sets the parent id of this document. Will simply set the routing to this value, as it is only
     * used for routing with delete requests.
     */
    public JestDeleteRequestBuilder parent(String parent) {
        if (routing == null) {
            routing = parent;
        }
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public JestDeleteRequestBuilder routing(String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    /**
     * Controls the shard routing of the delete request. Using this value to hash the shard
     * and not the id.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * Should a refresh be executed post this index operation causing the operation to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public JestDeleteRequestBuilder refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    /**
     * Sets the version, which will cause the delete operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public JestDeleteRequestBuilder version(long version) {
        this.version = version;
        return this;
    }

    public long version() {
        return this.version;
    }

    public JestDeleteRequestBuilder versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public VersionType versionType() {
        return this.versionType;
    }

    @Nullable
    public JestResult jestXcute(){
        final Delete build = getAction();

        try {
            return IndexClient.client.execute(build);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Delete getAction() {
        return new Delete.Builder(id).refresh(refresh).type(type).index(index).build();
    }
}
