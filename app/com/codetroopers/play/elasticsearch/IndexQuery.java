package com.codetroopers.play.elasticsearch;

import com.codetroopers.play.elasticsearch.jest.JestRichResult;
import com.codetroopers.play.elasticsearch.jest.JestSearchRequestBuilder;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.searchbox.core.search.facet.Facet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import play.Logger;
import play.libs.F;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * An ElasticSearch query
 *
 * @param <T> extends Index
 */
public class IndexQuery<T extends Index> {

    /**
     * Objet retourné dans les résultats
     */
    private final Class<T> clazz;

    /**
     * Query searchRequestBuilder
     */
    private QueryBuilder builder = QueryBuilders.matchAllQuery();
    private String query = null;
    private List<FacetBuilder> facets = new ArrayList<>();
    private List<SortBuilder> sorts = new ArrayList<>();

    private int from = -1;
    private int size = -1;
    private boolean explain = false;
    private boolean noField = false;

    public IndexQuery(Class<T> clazz) {
        Validate.notNull(clazz, "clazz cannot be null");
        this.clazz = clazz;
    }

    public IndexQuery<T> setBuilder(QueryBuilder builder) {
        this.builder = builder;

        return this;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setNoField(boolean noField) {
        this.noField = noField;
    }

    /**
     * Sets from
     *
     * @param from record index to start from
     * @return self
     */
    public IndexQuery<T> from(int from) {
        this.from = from;

        return this;
    }

    /**
     * Sets fetch size
     *
     * @param size the fetch size
     * @return self
     */
    public IndexQuery<T> size(int size) {
        this.size = size;

        return this;
    }

    public IndexQuery<T> setExplain(boolean explain) {
        this.explain = explain;

        return this;
    }

    /**
     * Adds a facet
     *
     * @param facet the facet
     * @return self
     */
    public IndexQuery<T> addFacet(FacetBuilder facet) {
        Validate.notNull(facet, "facet cannot be null");
        facets.add(facet);

        return this;
    }

    /**
     * Sorts the result by a specific field
     *
     * @param field the sort field
     * @param order the sort order
     * @return self
     */
    public IndexQuery<T> addSort(String field, SortOrder order) {
        Validate.notEmpty(field, "field cannot be null");
        Validate.notNull(order, "order cannot be null");
        sorts.add(SortBuilders.fieldSort(field).order(order));

        return this;
    }

    /**
     * Adds a generic {@link SortBuilder}
     *
     * @param sort the sort searchRequestBuilder
     * @return self
     */
    public IndexQuery<T> addSort(SortBuilder sort) {
        Validate.notNull(sort, "sort cannot be null");
        sorts.add(sort);

        return this;
    }

    /**
     * Runs the query
     *
     * @param indexQueryPath
     * @return
     */
    public IndexResults<T> fetch(IndexQueryPath indexQueryPath) {
        return fetch(indexQueryPath, null);
    }

    /**
     * Runs the query with a filter
     *
     * @param indexQueryPath
     * @param filter
     * @return
     */
    public IndexResults<T> fetch(IndexQueryPath indexQueryPath, FilterBuilder filter) {

        JestSearchRequestBuilder request = getSearchRequestBuilder(indexQueryPath, filter);

        return executeSearchRequest(request);
    }

    /**
     * Runs the query asynchronously
     *
     * @param indexQueryPath
     * @return
     */
    public F.Promise<IndexResults<T>> fetchAsync(IndexQueryPath indexQueryPath) {
        return fetchAsync(indexQueryPath, null);
    }

    /**
     * Runs the query asynchronously with a filter
     *
     * @param indexQueryPath
     * @param filter
     * @return
     */
    public F.Promise<IndexResults<T>> fetchAsync(final IndexQueryPath indexQueryPath, final FilterBuilder filter) {
        final F.Promise<JestRichResult> jestResultPromise = getSearchRequestBuilder(indexQueryPath, filter).executeAsync();
        return jestResultPromise.map(new F.Function<JestRichResult, IndexResults<T>>() {
            @Override
            public IndexResults<T> apply(JestRichResult jestResult) throws Throwable {
                return toSearchResults(jestResult);
            }
        });
    }

    public IndexResults<T> executeSearchRequest(JestSearchRequestBuilder request) {

        JestRichResult searchResponse = request.execute();

        if (IndexClient.config.showRequest && searchResponse != null) {
            Logger.debug("ElasticSearch : Response -> " + searchResponse.getJsonString());
        }

        return toSearchResults(searchResponse);
    }

    public JestSearchRequestBuilder getSearchRequestBuilder(IndexQueryPath indexQueryPath) {
        return getSearchRequestBuilder(indexQueryPath, null);
    }

    public JestSearchRequestBuilder getSearchRequestBuilder(FilterBuilder filter) {
        return getSearchRequestBuilder(null, filter);
    }

    public JestSearchRequestBuilder getSearchRequestBuilder(@Nullable IndexQueryPath indexQueryPath, FilterBuilder filter) {

        // Build request
        JestSearchRequestBuilder request = new JestSearchRequestBuilder();
        if (indexQueryPath != null) {
            request.setIndices(indexQueryPath.index)
                    .setTypes(indexQueryPath.type);
        }
        request.setSearchType(io.searchbox.params.SearchType.QUERY_THEN_FETCH)
                .setFilter(filter);

        // set Query
        if (StringUtils.isNotBlank(query)) {
            request.setQuery(query);
        } else {
            request.setQuery(builder);
        }

        // set no Fields -> only return id and type
        if (noField) {
            request.setNoFields();
        }

        // Facets
        for (FacetBuilder facet : facets) {
            request.addFacet(facet);
        }

        // Sorting
        for (SortBuilder sort : sorts) {
            request.addSort(sort);
        }

        // Paging
        if (from > -1) {
            request.setFrom(from);
        }
        if (size > -1) {
            request.setSize(size);
        }

        // Explain
        if (explain) {
            request.setExplain(true);
        }

        if (IndexClient.config.showRequest) {
            if (StringUtils.isNotBlank(query)) {
                Logger.debug("ElasticSearch : Query -> " + query);
            } else {
                Logger.debug("ElasticSearch : Query -> " + builder.toString());
            }
        }
        return request;
    }

    private IndexResults<T> toSearchResults(@NotNull JestRichResult jestRichResult) {
        List<T> results = Lists.newArrayList();
        long count = jestRichResult.getTotalHits();
        List<Facet> facetsResponse = jestRichResult.getFacets();
        for (JestRichResult.Result h : jestRichResult.getHits()) {
            results.add(h.getObject(clazz));
        }
        if (Logger.isDebugEnabled()) {
            Logger.debug("ElasticSearch : Results -> " + Joiner.on(",").join(results));
        }
        long pageSize = 10;
        if (size > -1) {
            pageSize = size;
        }

        long pageCurrent = 1;
        if (from > 0) {
            pageCurrent = ((int) (from / pageSize)) + 1;
        }

        long pageNb;
        if (pageSize == 0) {
            pageNb = 1;
        } else {
            pageNb = (long) Math.ceil(new BigDecimal(count).divide(new BigDecimal(pageSize), 2, RoundingMode.HALF_UP).doubleValue());
        }

        return new IndexResults<>(count, pageSize, pageCurrent, pageNb, results, facetsResponse);
    }

}

