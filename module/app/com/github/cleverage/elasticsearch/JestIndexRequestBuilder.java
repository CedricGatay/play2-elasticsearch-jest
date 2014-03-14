package com.github.cleverage.elasticsearch;

import io.searchbox.client.JestResult;
import io.searchbox.core.Index;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * @author cgatay
 */
class JestIndexRequestBuilder implements JestRequest<Index> {
    private final String index;
    private String type;
    private String id;
    private Object source;

    public JestIndexRequestBuilder(String index) {
        this.index = index;
    }

    @Override
    public Index getAction() {
        return new Index.Builder(source)
                    .index(index)
                    .type(type)
                    .id(id).build();
    }

    public JestIndexRequestBuilder setType(String type) {
        this.type = type;
        return this;
    }

    public String getType() {
        return type;
    }

    public JestIndexRequestBuilder setId(String id) {
        this.id = id;
        return this;
    }

    public String getId() {
        return id;
    }

    public JestIndexRequestBuilder setSource(Object source) {
        this.source = source;
        return this;
    }

    public Object getSource() {
        return source;
    }
}
