package com.codetroopers.play.elasticsearch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.codetroopers.play.elasticsearch.jest.JestRichResult;
import io.searchbox.annotations.JestId;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.SearchHit;

import play.libs.F;

import com.codetroopers.play.elasticsearch.annotations.IndexName;
import com.codetroopers.play.elasticsearch.annotations.IndexType;

import java.util.Map;

import static com.codetroopers.play.elasticsearch.IndexService.*;

@JsonIgnoreProperties({"searchHit"})
public abstract class Index implements Indexable {

    @JestId
    public String id;

    /**
     * SearchHit is use when do a search
     * It's contains information like "_index", "_type", "_id", "_score", "hightlight", ...
     */
    public SearchHit searchHit;

    /**
     * Return indexNames and indexType
     * @return
     */
    public IndexQueryPath getIndexPath() {

        IndexType indexTypeAnnotation = this.getClass().getAnnotation(IndexType.class);
        if(indexTypeAnnotation == null) {
            throw new ElasticsearchException("ElasticSearch : Class " + this.getClass().getCanonicalName() + " no contain @IndexType(name) annotation ");
        }
        String indexType = indexTypeAnnotation.name();

        String indexName = INDEX_DEFAULT;
        IndexName indexNameAnnotation = this.getClass().getAnnotation(IndexName.class);
        if(indexNameAnnotation != null) {
            indexName = indexNameAnnotation.name();
        }

        return new IndexQueryPath(indexName, indexType);
    }

    /**
     * Return indexQueryPath for a specific indexName
     * @return
     */
    public IndexQueryPath getIndexPath(String indexName) {

        IndexQueryPath queryPath = getIndexPath();
        queryPath.index = indexName;
        return queryPath;
    }

    /**
     * Index this Document
     * @return
     * @throws Exception
     */
    public JestRichResult index() {
        return IndexService.index(getIndexPath(), id, this);
    }

    /**
     * Index this Document on this indexName
     * @return
     * @throws Exception
     */
    public JestRichResult index(String indexName) {
        return IndexService.index(getIndexPath(indexName), id, this);
    }

    /**
     * Index this Document asynchronously
     * @return
     * @throws Exception
     */
    public F.Promise<JestRichResult> indexAsync() {
        return IndexService.indexAsync(getIndexPath(), id, this);
    }

    /**
     * Index this Document asynchronously
     * @return
     * @throws Exception
     */
    public F.Promise<JestRichResult> indexAsync(String indexName) {
        return IndexService.indexAsync(getIndexPath(indexName), id, this);
    }

    public JestRichResult update(Map<String, Object> updateFieldValues, String updateScript){
        return IndexService.update(getIndexPath(), id, updateFieldValues, updateScript);
    }

    public JestRichResult update(String indexName, Map<String, Object> updateFieldValues, String updateScript){
        return IndexService.update(getIndexPath(indexName), id, updateFieldValues, updateScript);
    }

    public F.Promise<JestRichResult> updateAsync(Map<String, Object> updateFieldValues, String updateScript){
        return IndexService.updateAsync(getIndexPath(), id, updateFieldValues, updateScript);
    }

    public F.Promise<JestRichResult> updateAsync(String indexName, Map<String, Object> updateFieldValues, String updateScript){
        return IndexService.updateAsync(getIndexPath(indexName), id, updateFieldValues, updateScript);
    }

    /**
     * Delete this Document
     * @return
     * @throws Exception
     */
    public JestRichResult delete() {
        return IndexService.delete(getIndexPath(), id);
    }

    /**
     * Delete this Document for this indexName
     * @return
     * @throws Exception
     */
    public JestRichResult delete(String indexName) {
        return IndexService.delete(getIndexPath(indexName), id);
    }

    /**
     * Delete this Document asynchronously
     * @return
     * @throws Exception
     */
    public F.Promise<JestRichResult> deleteAsync() {
        return IndexService.deleteAsync(getIndexPath(), id);
    }

    /**
     * Delete this Document asynchronously for this indexName
     * @return
     * @throws Exception
     */
    public F.Promise<JestRichResult> deleteAsync(String indexName) {
        return IndexService.deleteAsync(getIndexPath(indexName), id);
    }

    /**
     * Helper for index queries.
     */
    public static class Finder<T extends Index> {

        private final Class<T> type;
        private final IndexQueryPath queryPath;

        /**
         * Creates a finder for document of type <code>T</code>
         * @param type
         */
        public Finder(Class<T> type) {
            this.type = type;
            T t = IndexUtils.getInstanceIndex(type);
            this.queryPath = t.getIndexPath();
        }

        /**
         * Creates a finder for document of type <code>T</code> for a specific indexName
         * @param type
         */
        public Finder(Class<T> type, String indexName) {
            this.type = type;
            T t = IndexUtils.getInstanceIndex(type);
            this.queryPath = t.getIndexPath(indexName);
        }

        /**
         * Return a query for request this Index
         * @return
         */
        public IndexQuery<T> query() {
            return new IndexQuery<>(type);
        }

        /**
         * Retrieves an entity by ID.
         * @param id
         * @return
         */
        public T byId(String id) {
            return get(queryPath, type, id);
        }

        /**
         * Retrieves all entities of the given type.
         */
        public IndexResults<T> all() {
            return search(query());
        }

        /**
         * Find method
         * @param query
         * @return
         * @throws Exception
         */
        public IndexResults<T> search(IndexQuery<T> query) {

            return IndexService.search(queryPath, query);
        }

        public F.Promise<IndexResults<T>> searchAsync(IndexQuery<T> query) {

            return IndexService.searchAsync(queryPath, query, null);
        }

        public F.Promise<IndexResults<T>> searchAsync(IndexQuery<T> query, FilterBuilder filter){
            return IndexService.searchAsync(queryPath, query, filter);
        }
    }


}
