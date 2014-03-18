package com.github.cleverage.elasticsearch;

import com.github.cleverage.elasticsearch.jest.*;
import io.searchbox.indices.*;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.percolator.PercolatorService;
import org.elasticsearch.index.query.FilterBuilder;
import play.Logger;
import play.libs.F;

import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.github.cleverage.elasticsearch.jest.JestClientWrapper.execute;
import static com.github.cleverage.elasticsearch.jest.JestClientWrapper.log;


public abstract class IndexService {

    public static final String INDEX_DEFAULT = IndexClient.config.indexNames[0];
    public static final String INDEX_PERCOLATOR = PercolatorService.INDEX_NAME;

    /**
     * get indexRequest to index from a specific request
     *
     * @return
     */
    public static JestIndexRequestBuilder getIndexRequest(IndexQueryPath indexPath, String id, Index indexable) {
        return new JestIndexRequestBuilder(indexPath.index)
                .setType(indexPath.type)
                .setId(id)
                .setSource(indexable.toIndex());
    }

    /**
     * index from an request
     *
     * @param requestBuilder
     * @return
     */
    @NotNull
    public static JestRichResult index(JestIndexRequestBuilder requestBuilder) {
        JestRichResult jestResult = execute(requestBuilder);
        log(jestResult, "index");
        return jestResult;
    }

    /**
     * Create an JestIndexRequestBuilder
     *
     * @param indexPath
     * @param id
     * @param indexable
     * @return
     */
    private static JestIndexRequestBuilder getJestIndexRequestBuilder(IndexQueryPath indexPath, String id, Index indexable) {
        return getIndexRequest(indexPath, id, indexable);
    }

    /**
     * Add Indexable object in the index
     *
     * @param indexPath
     * @param indexable
     * @return
     */
    @NotNull
    public static JestRichResult index(IndexQueryPath indexPath, String id, Index indexable) {
        JestRichResult jestResult = getJestIndexRequestBuilder(indexPath, id, indexable).execute();
        log(jestResult, "index");
        return jestResult;
    }

    /**
     * Add Indexable object in the index asynchronously
     *
     * @param indexPath
     * @param indexable
     * @return
     */
    public static F.Promise<JestRichResult> indexAsync(IndexQueryPath indexPath, String id, Index indexable) {
        return getJestIndexRequestBuilder(indexPath, id, indexable).executeAsync();
    }

    /**
     * call JestIndexRequestBuilder on asynchronously
     *
     * @param jestIndexRequestBuilder
     * @return
     */
    public static F.Promise<JestRichResult> indexAsync(JestIndexRequestBuilder jestIndexRequestBuilder) {
        return jestIndexRequestBuilder.executeAsync();
    }

    /**
     * Add a json document to the index
     *
     * @param indexPath
     * @param id
     * @param json
     * @return
     */
    @NotNull
    public static JestRichResult index(IndexQueryPath indexPath, String id, String json) {
        return getJestIndexRequestBuilder(indexPath, id, json).execute();
    }

    /**
     * Create an JestIndexRequestBuilder for a Json-encoded object
     *
     * @param indexPath
     * @param id
     * @param json
     * @return
     */
    public static JestIndexRequestBuilder getJestIndexRequestBuilder(IndexQueryPath indexPath, String id, String json) {
        return new JestIndexRequestBuilder(indexPath.index)
                .setType(indexPath.type)
                .setId(id)
                .setSource(json);
    }

    /**
     * Create a BulkRequestBuilder for a List of Index objects
     *
     * @param indexPath
     * @param indexables
     * @return
     */
    private static JestBulkRequestBuilder getBulkRequestBuilder(IndexQueryPath indexPath, List<? extends Index> indexables) {
        final JestBulkRequestBuilder jestBulkRequestBuilder = new JestBulkRequestBuilder();
        for (Index indexable : indexables) {
            jestBulkRequestBuilder.add(getJestIndexRequestBuilder(indexPath, indexable.id, indexable).getAction());
        }
        return jestBulkRequestBuilder;
    }

    /**
     * Bulk index a list of indexables
     *
     * @param indexPath
     * @param indexables
     * @return
     */
    public static JestRichResult indexBulk(IndexQueryPath indexPath, List<? extends Index> indexables) {
        JestBulkRequestBuilder bulkRequestBuilder = getBulkRequestBuilder(indexPath, indexables);
        return bulkRequestBuilder.execute();
    }

    /**
     * Bulk index a list of indexables asynchronously
     *
     * @param indexPath
     * @param indexables
     * @return
     */
    public static F.Promise<JestRichResult> indexBulkAsync(IndexQueryPath indexPath, List<? extends Index> indexables) {
        return getBulkRequestBuilder(indexPath, indexables).executeAsync();
    }

    /**
     * Create a BulkRequestBuilder for a List of json-encoded objects
     *
     * @param indexPath
     * @param jsonMap
     * @return
     */
    public static JestBulkRequestBuilder getBulkRequestBuilder(IndexQueryPath indexPath, Map<String, String> jsonMap) {
        JestBulkRequestBuilder bulkRequestBuilder = new JestBulkRequestBuilder();
        for (String id : jsonMap.keySet()) {
            bulkRequestBuilder.add(getJestIndexRequestBuilder(indexPath, id, jsonMap.get(id)));
        }
        return bulkRequestBuilder;
    }

    /**
     * Bulk index a list of indexables asynchronously
     *
     * @param bulkRequestBuilder
     * @return
     */
    public static F.Promise<JestRichResult> indexBulkAsync(JestBulkRequestBuilder bulkRequestBuilder) {
        return bulkRequestBuilder.executeAsync();
    }

    /**
     * Create a BulkRequestBuilder for a List of JestIndexRequestBuilder
     *
     * @return
     */
    public static JestBulkRequestBuilder getBulkRequestBuilder(Collection<JestIndexRequestBuilder> JestIndexRequestBuilder) {
        JestBulkRequestBuilder bulkRequestBuilder = new JestBulkRequestBuilder();
        for (JestIndexRequestBuilder requestBuilder : JestIndexRequestBuilder) {
            bulkRequestBuilder.add(requestBuilder);
        }
        return bulkRequestBuilder;
    }

    /**
     * Bulk index a Map of json documents.
     * The id of the document is the key of the Map
     *
     * @param indexPath
     * @param jsonMap
     * @return
     */
    public static JestRichResult indexBulk(IndexQueryPath indexPath, Map<String, String> jsonMap) {
        JestBulkRequestBuilder bulkRequestBuilder = getBulkRequestBuilder(indexPath, jsonMap);
        return bulkRequestBuilder.execute();
    }

    /**
     * Create an UpdateRequestBuilder
     *
     * @param indexPath
     * @param id
     * @return
     */
    public static JestUpdateRequestBuilder getUpdateRequestBuilder(IndexQueryPath indexPath,
                                                                   String id,
                                                                   Map<String, Object> updateFieldValues,
                                                                   String updateScript) {
        return new JestUpdateRequestBuilder(indexPath.index, indexPath.type, id).setScriptParams(updateFieldValues).setScript(updateScript);
    }

    /**
     * Update a document in the index
     *
     * @param indexPath
     * @param id
     * @param updateFieldValues The fields and new values for which the update should be done
     * @param updateScript
     * @return
     */
    public static JestRichResult update(IndexQueryPath indexPath,
                                        String id,
                                        Map<String, Object> updateFieldValues,
                                        String updateScript) {
        return getUpdateRequestBuilder(indexPath, id, updateFieldValues, updateScript).execute();
    }

    /**
     * Update a document asynchronously
     *
     * @param indexPath
     * @param id
     * @param updateFieldValues The fields and new values for which the update should be done
     * @param updateScript
     * @return
     */
    public static F.Promise<JestRichResult> updateAsync(IndexQueryPath indexPath,
                                                        String id,
                                                        Map<String, Object> updateFieldValues,
                                                        String updateScript) {
        return getUpdateRequestBuilder(indexPath, id, updateFieldValues, updateScript).executeAsync();
    }

    /**
     * Call update asynchronously
     *
     * @param updateRequestBuilder
     * @return
     */
    public static F.Promise<JestRichResult> updateAsync(JestUpdateRequestBuilder updateRequestBuilder) {
        return updateRequestBuilder.executeAsync();
    }

    /**
     * Create a DeleteRequestBuilder
     *
     * @param indexPath
     * @param id
     * @return
     */
    public static JestDeleteRequestBuilder getDeleteRequestBuilder(IndexQueryPath indexPath, String id) {
        return new JestDeleteRequestBuilder(indexPath.index, indexPath.type, id);
    }

    /**
     * Delete element in index asynchronously
     *
     * @param indexPath
     * @return
     */
    public static F.Promise<JestRichResult> deleteAsync(IndexQueryPath indexPath, String id) {
        return getDeleteRequestBuilder(indexPath, id).executeAsync();
    }

    /**
     * Delete element in index
     *
     * @param indexPath
     * @return
     */
    @NotNull
    public static JestRichResult delete(IndexQueryPath indexPath, String id) {
        JestRichResult deleteResponse = getDeleteRequestBuilder(indexPath, id).execute();
        log(deleteResponse, "delete");
        return deleteResponse;
    }

    /**
     * Create a GetRequestBuilder
     *
     * @param indexPath
     * @param id
     * @return
     */
    public static JestGetRequestBuilder getGetRequestBuilder(IndexQueryPath indexPath, String id) {
        return new JestGetRequestBuilder(indexPath.index, indexPath.type, id);
    }

    /**
     * Get the json representation of a document from an id
     *
     * @param indexPath
     * @param id
     * @return
     */
    @NotNull
    public static String getAsString(IndexQueryPath indexPath, String id) {
        final JestRichResult jestResult = getGetRequestBuilder(indexPath, id).execute();
        if (jestResult != null) {
            return jestResult.getJsonString();
        }
        return "";
    }

    @NotNull
    private static <T extends Index> T getTFromGetResponse(Class<T> clazz, JestRichResult getResponse) {
        if (!getResponse.isSucceeded()) {
            return null;
        }
        return getResponse.getFirstHit(clazz);
    }

    /**
     * Get Indexable Object for an Id
     *
     * @param indexPath
     * @param clazz
     * @return
     */
    public static <T extends Index> T get(IndexQueryPath indexPath, Class<T> clazz, String id) {
        JestGetRequestBuilder getRequestBuilder = getGetRequestBuilder(indexPath, id);
        return getTFromGetResponse(clazz, getRequestBuilder.execute());
    }

    /**
     * Get Indexable Object for an Id asynchronously
     *
     * @param indexPath
     * @param clazz
     * @param id
     * @param <T>
     * @return
     */
    @NotNull
    public static <T extends Index> F.Promise<T> getAsync(IndexQueryPath indexPath, final Class<T> clazz, String id) {
        final F.Promise<JestRichResult> jestResultPromise = getGetRequestBuilder(indexPath, id).executeAsync();
        return jestResultPromise.map(
                new F.Function<JestRichResult, T>() {
                    public T apply(JestRichResult getResponse) {
                        return getTFromGetResponse(clazz, getResponse);
                    }
                }
        );
    }

    /**
     * Get a reponse for a simple request
     *
     * @param indexName
     * @param indexType
     * @param id
     * @return
     */
    public static JestRichResult get(String indexName, String indexType, String id) {
        return new JestGetRequestBuilder(indexName, indexType, id).execute();
    }

    /**
     * Search information on Index from a query
     *
     * @param indexQuery
     * @param <T>
     * @return
     */
    public static <T extends Index> IndexResults<T> search(IndexQueryPath indexPath, IndexQuery<T> indexQuery) {
        return indexQuery.fetch(indexPath);
    }

    /**
     * Search asynchronously information on Index from a query
     *
     * @param indexPath
     * @param indexQuery
     * @param <T>
     * @return
     */
    public static <T extends Index> F.Promise<IndexResults<T>> searchAsync(IndexQueryPath indexPath,
                                                                           IndexQuery<T> indexQuery,
                                                                           FilterBuilder filter) {
        return indexQuery.fetchAsync(indexPath, filter);
    }

    /**
     * Test if an indice Exists
     *
     * @return true if exists
     */
    public static boolean existsIndex(String indexName) {
        final JestRichResult jestResult = execute(new IndicesExists.Builder(indexName).build());
        return jestResult != null && jestResult.isSucceeded();
    }

    /**
     * Create the index
     */
    public static void createIndex(String indexName) {
        Logger.debug("ElasticSearch : creating index [" + indexName + "]");
        final String jsonNode = IndexClient.config.indexSettings.get(indexName);
        final CreateIndex.Builder builder = new CreateIndex.Builder(indexName);
        if (StringUtils.isNotBlank(jsonNode)) {
            final ImmutableMap<String, String> settings = ImmutableSettings.builder().loadFromSource(jsonNode).build().getAsMap();
            builder.settings(settings);
        }
        final JestRichResult jestResult = execute(builder.build());
        log(jestResult, "index creation result =>");
    }

    /**
     * Delete the index
     */
    public static void deleteIndex(String indexName) {
        Logger.debug("ElasticSearch : deleting index [" + indexName + "]");
        final DeleteIndex.Builder builder = new DeleteIndex.Builder(indexName);
        execute(builder.build());
    }

    /**
     * Create Mapping ( for example mapping type : nested, geo_point  )
     * see http://www.elasticsearch.org/guide/reference/mapping/
     * <p/>
     * {
     * "tweet" : {
     * "properties" : {
     * "message" : {"type" : "string", "store" : "yes"}
     * }
     * }
     * }
     *  @param indexName
     * @param indexType
     * @param indexMapping
     */
    public static JestRichResult createMapping(String indexName, String indexType, String indexMapping) {
        Logger.debug("ElasticSearch : creating mapping [" + indexName + "/" + indexType + "] :  " + indexMapping);
        final PutMapping build = new PutMapping.Builder(indexName, indexType, indexMapping).build();
        return execute(build);
    }

    /**
     * Read the Mapping for a type
     *
     * @param indexType
     * @return
     */
    @NotNull
    public static String getMapping(String indexName, String indexType) {
        final GetMapping build = new GetMapping.Builder().addIndex(indexName).addType(indexType).build();
        final JestRichResult jestResult = execute(build);
        if (jestResult != null) {
            return jestResult.getJsonString();
        }
        return "";
    }

    /**
     * call createMapping for list of @indexType
     *
     * @param indexName
     */
    public static void prepareIndex(String indexName) {
        Map<IndexQueryPath, String> indexMappings = IndexClient.config.indexMappings;
        for (IndexQueryPath indexQueryPath : indexMappings.keySet()) {
            if (indexName != null && indexName.equals(indexQueryPath.index)) {
                String indexType = indexQueryPath.type;
                String indexMapping = indexMappings.get(indexQueryPath);
                if (indexMapping != null) {
                    createMapping(indexName, indexType, indexMapping);
                }
            }
        }
    }

    public static void cleanIndex() {

        String[] indexNames = IndexClient.config.indexNames;
        for (String indexName : indexNames) {
            cleanIndex(indexName);
        }
    }

    public static void cleanIndex(String indexName) {

        if (IndexService.existsIndex(indexName)) {
            IndexService.deleteIndex(indexName);
        }
        IndexService.createIndex(indexName);
        IndexService.prepareIndex(indexName);
    }

    /**
     * Refresh full index
     */
    public static void refresh() {
        String[] indexNames = IndexClient.config.indexNames;
        for (String indexName : indexNames) {
            refresh(indexName);
        }
    }

    /**
     * Refresh an index
     *
     * @param indexName
     */
    private static void refresh(String indexName) {
        execute(new Refresh.Builder().addIndex(indexName).build());
    }

    /**
     * Flush full index
     */
    public static void flush() {
        String[] indexNames = IndexClient.config.indexNames;
        for (String indexName : indexNames) {
            flush(indexName);
        }
    }

    /**
     * Flush an index
     *
     * @param indexName
     */
    public static void flush(String indexName) {
        execute(new Flush.Builder().addIndex(indexName).build());
    }

}
