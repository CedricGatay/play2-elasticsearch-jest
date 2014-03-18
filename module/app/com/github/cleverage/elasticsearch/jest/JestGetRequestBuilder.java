package com.github.cleverage.elasticsearch.jest;

import io.searchbox.core.Get;

/**
 * @author cgatay
 */
public class JestGetRequestBuilder implements JestRequest<Get>{
    private final String index;
    private final String type;
    private final String id;

    public JestGetRequestBuilder(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    public Get getAction() {
        return new Get.Builder(index, id).type(type).build();
    }
}
