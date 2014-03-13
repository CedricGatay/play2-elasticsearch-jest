package com.github.cleverage.elasticsearch;

import io.searchbox.client.JestResult;
import io.searchbox.core.Index;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;

import javax.annotation.Nullable;

/**
 * @author cgatay
 */
class JestIndexRequestBuilder extends IndexRequestBuilder {
    private final String index;

    public JestIndexRequestBuilder(String index) {
        super(null);
        this.index = index;
    }

    @Nullable
    public JestResult jestXcute(){
        final Index indexRequest = getAction();

        try {
            return IndexClient.client.execute(indexRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Index getAction() {
        return new Index.Builder(request.sourceAsMap())
                    .index(index)
                    .type(request.type())
                    .id(request.id()).build();
    }

    @Override
    protected void doExecute(final ActionListener<IndexResponse> listener) {
        throw new RuntimeException("Should not be called");
    }

    @Override
    public ListenableActionFuture<IndexResponse> execute() {
        throw new RuntimeException("Should not be called");
    }
}
