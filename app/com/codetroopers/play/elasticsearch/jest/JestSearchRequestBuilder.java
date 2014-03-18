package com.codetroopers.play.elasticsearch.jest;

import io.searchbox.core.Search;
import io.searchbox.params.SearchType;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.sort.SortBuilder;

/**
 * @author cgatay
 */
public class JestSearchRequestBuilder extends JestRequest<Search> {

    private SearchSourceBuilder searchSourceBuilder;
    private String indices;
    private String types;
    private SearchType searchType;

    public JestSearchRequestBuilder() {
        searchSourceBuilder = new SearchSourceBuilder();
    }

    @Override
    public Search getAction() {
        final String query = searchSourceBuilder.toString();
        return new Search.Builder(query)
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
