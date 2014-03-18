package com.github.cleverage.elasticsearch;

import com.github.cleverage.elasticsearch.jest.JestClientWrapper;
import com.github.cleverage.elasticsearch.jest.JestResultUtils;
import com.github.cleverage.elasticsearch.jest.JestSearchRequestBuilder;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.searchbox.client.JestResult;
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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

import static com.github.cleverage.elasticsearch.jest.JestClientWrapper.executeAsync;

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
        final F.Promise<JestResult> jestResultPromise = executeAsync(getSearchRequestBuilder(indexQueryPath, filter).getAction());
        return jestResultPromise.map(new F.Function<JestResult, IndexResults<T>>() {
            @Override
            public IndexResults<T> apply(JestResult jestResult) throws Throwable {
                return toSearchResults(jestResult);
            }
        });
    }

    public IndexResults<T> executeSearchRequest(JestSearchRequestBuilder request) {

        JestResult searchResponse = JestClientWrapper.execute(request);

        if (IndexClient.config.showRequest && searchResponse != null) {
            Logger.debug("ElasticSearch : Response -> " + searchResponse.getJsonString());
        }

        return toSearchResults(searchResponse);
    }

    public JestSearchRequestBuilder getSearchRequestBuilder(IndexQueryPath indexQueryPath) {
        return getSearchRequestBuilder(indexQueryPath, null);
    }

    public JestSearchRequestBuilder getSearchRequestBuilder(IndexQueryPath indexQueryPath, FilterBuilder filter) {

        // Build request
        JestSearchRequestBuilder request = new JestSearchRequestBuilder()
                .setIndices(indexQueryPath.index)
                .setTypes(indexQueryPath.type)
                .setSearchType(io.searchbox.params.SearchType.QUERY_THEN_FETCH)
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

    private IndexResults<T> toSearchResults(@Nullable JestResult searchResponse) {
        long count = 0;
        List<Facet> facetsResponse = Lists.newArrayList();
        List<T> results = Lists.newArrayList();
        if (searchResponse != null) {
            final JestResultUtils jestResultUtils = new JestResultUtils(searchResponse);
            count = jestResultUtils.getTotalHits();
            facetsResponse = jestResultUtils.getFacets();
            for (JestResultUtils.Result h : jestResultUtils.getHits()) {
                results.add(h.getObject(clazz));
            }
            if (Logger.isDebugEnabled()) {
                Logger.debug("ElasticSearch : Results -> " + Joiner.on(",").join(results));
            }
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

