package com.github.cleverage.elasticsearch;

import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.core.Search;
import io.searchbox.params.SearchType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * @author cgatay
 */
class JestSearchRequestBuilder extends SearchRequestBuilder {
    public JestSearchRequestBuilder() {
        super(null);
        
    }

    @Nullable
    public JestResult jestXcute(){
        final SearchSourceBuilder searchSourceBuilder = internalBuilder();
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(Arrays.asList(request.indices()))
                .addType(Arrays.asList(request.types()))
                        //TODO du beau
                .setSearchType(SearchType.valueOf(request.searchType().name()))
                .build();
        try {
            return IndexClient.client.execute(search);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected void doExecute(final ActionListener<SearchResponse> listener) {
        throw new RuntimeException("Should not be called");
    }

    @Override
    public ListenableActionFuture<SearchResponse> execute() {
        throw new RuntimeException("Should not be called");
    }
}
