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
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * @author cgatay
 */
class JestSearchRequestBuilder implements JestRequest<Search> {

    private SearchSourceBuilder searchSourceBuilder;
    private String indices;
    private String types;
    private SearchType searchType;

    public JestSearchRequestBuilder() {
        searchSourceBuilder = new SearchSourceBuilder();
    }

    @Override
    public Search getAction() {
        return new Search.Builder(searchSourceBuilder.toString())
                .addIndex(indices)
                .addType(types)
                .setSearchType(searchType)
                .build();
    }

    public JestSearchRequestBuilder setIndices(String indices) {
        this.indices = indices;
        return this;
    }

    public JestSearchRequestBuilder setTypes(String types) {
        this.types = types;
        return this;
    }

    public JestSearchRequestBuilder setSearchType(SearchType searchType) {
        this.searchType = searchType;
        return this;
    }

    public JestSearchRequestBuilder setFilter(FilterBuilder filter) {
        this.searchSourceBuilder.postFilter(filter);
        return this;
    }

    public JestSearchRequestBuilder setQuery(String query) {
        this.searchSourceBuilder.query(query);
        return this;
    }

    public JestSearchRequestBuilder setQuery(QueryBuilder builder) {
        this.searchSourceBuilder.query(builder);
        return this;
    }

    public JestSearchRequestBuilder setNoFields() {
        this.searchSourceBuilder.noFields();
        return this;
    }

    public JestSearchRequestBuilder addFacet(FacetBuilder facet) {
        this.searchSourceBuilder.facet(facet);
        return this;
    }

    public JestSearchRequestBuilder addSort(SortBuilder sort) {
        this.searchSourceBuilder.sort(sort);
        return this;
    }

    public JestSearchRequestBuilder setFrom(int from) {
        this.searchSourceBuilder.from(from);
        return this;
    }

    public JestSearchRequestBuilder setSize(int size) {
        this.searchSourceBuilder.size(size);
        return this;
    }

    public JestSearchRequestBuilder setExplain(boolean explain) {
        this.searchSourceBuilder.explain(explain);
        return this;
    }

    
}
