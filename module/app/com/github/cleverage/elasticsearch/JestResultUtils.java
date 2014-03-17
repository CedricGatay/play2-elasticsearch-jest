package com.github.cleverage.elasticsearch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.*;
import io.searchbox.client.JestResult;
import io.searchbox.core.search.facet.*;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

/**
 * @author cgatay
 */
public class JestResultUtils {
    private static final Gson gson = createGsonWithDateFormat();
    private static final Map<String, Class<? extends Facet>> FACET_TYPE_CLASSES;
    private final JestResult result;

    static{
        ImmutableMap.Builder<String, Class<? extends Facet>> tmp = ImmutableMap.builder();
        tmp.put(QueryFacet.TYPE.toLowerCase(), QueryFacet.class);
        tmp.put(StatisticalFacet.TYPE.toLowerCase(), StatisticalFacet.class);
        tmp.put(TermsStatsFacet.TYPE.toLowerCase(), TermsStatsFacet.class);
        tmp.put(DateHistogramFacet.TYPE.toLowerCase(), DateHistogramFacet.class);
        tmp.put(GeoDistanceFacet.TYPE.toLowerCase(), GeoDistanceFacet.class);
        tmp.put(FilterFacet.TYPE.toLowerCase(), FilterFacet.class);
        tmp.put(TermsFacet.TYPE.toLowerCase(), TermsFacet.class);
        tmp.put(RangeFacet.TYPE.toLowerCase(), RangeFacet.class);
        tmp.put(HistogramFacet.TYPE.toLowerCase(), HistogramFacet.class);
        FACET_TYPE_CLASSES = tmp.build();
    }
    
    public JestResultUtils(JestResult result) {
        this.result = result;
    }

    public List<Facet> getFacets(){
        List<Facet> out = Lists.newArrayList();
        final JsonObject jsonObject = result.getJsonObject();
        if (jsonObject != null) {
            final JsonElement facets = jsonObject.get("facets");
            if (facets != null) {
                for (Map.Entry<String, JsonElement> facetEntry : ((JsonObject) facets).entrySet()) {
                    JsonObject facet = facetEntry.getValue().getAsJsonObject();
                    final JsonElement jsonElement = facet.get("_type");
                    if (jsonElement != null){
                        final Class<? extends Facet> facetClazz = FACET_TYPE_CLASSES.get(jsonElement.getAsString().toLowerCase());

                        try {
                            final Constructor<? extends Facet> constructor = facetClazz.getConstructor(String.class, JsonObject.class);
                            out.add(constructor.newInstance(facetEntry.getKey(), facetEntry.getValue()));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                }
            }
        }
        return out;
    }

    public int getTotalHits(){
        final JsonElement hits = result.getJsonObject().get("hits");
        if(hits != null) {
            final JsonElement total = hits.getAsJsonObject().get("total");
            if (total != null){
                return total.getAsInt();
            }
        }
        return 0;
    }

/*
 *
 {"took":24,"timed_out":false,"_shards":{"total":5,"successful":5,"failed":0},
 "hits":{"total":2,"max_score":1.0,
 "hits":[
 {"_index":"index1","_type":"type1","_id":"q04MS2ITT2-GT_T3kdg_FA","_score":1.0, "_source" : {"name":"name1","category":"category","dateCreate":"2014-03-17T12:36:25Z"}},
 {"_index":"index1","_type":"type1","_id":"P-vu7EsVQQmrut7-Q_X79Q","_score":1.0, "_source" : {"name":"name1","category":"category","dateCreate":"2014-03-17T12:36:25Z"}}]
 }
 }
 */
    public List<Result> getHits(){
        List<Result> out = Lists.newArrayList();
        final JsonElement hits = result.getJsonObject().get("hits");
        if (hits != null){
            final JsonElement hitsList = hits.getAsJsonObject().get("hits");
            if (hitsList != null){
                final JsonArray jsonArray = hitsList.getAsJsonArray();
                for (JsonElement jsonElement : jsonArray) {
                    out.add(new Result(jsonElement.getAsJsonObject()));
                }
            }
        }
        return out;
    }

    @Nullable
    public <T extends Index> T getFirstHit(Class<T> clazz) {
        if (result.isSucceeded()){
            return new Result(result.getJsonObject()).getObject(clazz);
        }
        return null;
    }

    @Nullable
    public String getId(){
        final JsonObject jsonObject = result.getJsonObject();
        if (jsonObject.has("_id")) {
            return jsonObject.get("_id").getAsString();
        }
        return null;
    }

    public static Gson createGsonWithDateFormat() {
        return new GsonBuilder()
                .setDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").create();
    }

    public static class Result{
        private final String index;
        private final String type;
        private final String id;
        private final Float score;
        private final JsonObject source;
        private transient Index convertedObject = null;

        public Result(JsonObject resultLine) {
            this.index = resultLine.get("_index").getAsString();
            this.type = resultLine.get("_type").getAsString();
            this.id = resultLine.get("_id").getAsString();
            this.score = resultLine.has("_score") ? resultLine.get("_score").getAsFloat() : 1.0f;
            this.source = resultLine.get("_source").getAsJsonObject();
        }
        
        public String index(){
            return index;
        }
        public String type(){
            return type;
        }
        public String id(){
            return id;
        }
        public Float score(){
            return score;
        }
        
        @SuppressWarnings("unchecked")
        public Map<String, Object> sourceAsMap(){
            return gson.fromJson(source, Map.class);
        }
        
        public JsonObject source(){
            return source;
        }

        @SuppressWarnings("unchecked")
        public <T extends Index> T getObject(Class<T> clazz){
            if (convertedObject == null) {
                convertedObject = gson.fromJson(source, clazz);
                convertedObject.id = id();
            }
            return (T)convertedObject;
        }
        
    }
}
