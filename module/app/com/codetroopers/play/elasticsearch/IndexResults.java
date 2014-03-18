package com.codetroopers.play.elasticsearch;

import io.searchbox.core.search.facet.Facet;

import java.util.List;

/**
 * The Class IndexResult.
 *
 * @param <T> extends Index
 */
public class IndexResults<T extends Index> {

    /** The total count. */
    public long totalCount;

    /**
     * Nb result by page
     */
    public long pageSize;

    /**
     * Number of current page
     */
    public long pageCurrent;

    /**
     * Number of total pages
     */
    public long pageNb;

    /** The results. */
    public List<T> results;

    /** The facets. */
    public List<? extends Facet> facets;

    /**
     * Create new Index Result
     * @param totalCount
     * @param pageSize
     * @param pageCurrent
     * @param pageNb
     * @param results
     * @param facets
     */
    public IndexResults(long totalCount,long pageSize, long pageCurrent, long pageNb, List<T> results, List<? extends Facet> facets) {
        this.totalCount = totalCount;
        this.pageSize = pageSize;
        this.pageCurrent = pageCurrent;
        this.pageNb = pageNb;
        this.results = results;
        this.facets = facets;
    }

}

