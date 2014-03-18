package com.github.cleverage.elasticsearch.jest;

import io.searchbox.core.Index;

/**
 * @author cgatay
 */
public class JestIndexRequestBuilder implements JestRequest<Index> {
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
