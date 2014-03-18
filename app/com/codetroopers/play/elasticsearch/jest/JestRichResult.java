package com.codetroopers.play.elasticsearch.jest;

import com.codetroopers.play.elasticsearch.Index;
import com.codetroopers.play.elasticsearch.IndexUtils;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.*;
import io.searchbox.client.JestResult;
import io.searchbox.core.search.facet.*;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

/**
 * @author cgatay
 */
public class JestRichResult {
    private static final Gson gson = createGsonWithDateFormat();
    private static final Map<String, Class<? extends Facet>> FACET_TYPE_CLASSES;
    private JestResult result;

    static {
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

    public JestRichResult(@Nullable JestResult result) {
        this.result = result;
    }

    public List<Facet> getFacets() {
        List<Facet> out = Lists.newArrayList();
        final JsonObject jsonObject = safeResult().getJsonObject();
        if (jsonObject != null) {
            final JsonElement facets = jsonObject.get("facets");
            if (facets != null) {
                for (Map.Entry<String, JsonElement> facetEntry : ((JsonObject) facets).entrySet()) {
                    JsonObject facet = facetEntry.getValue().getAsJsonObject();
                    final JsonElement jsonElement = facet.get("_type");
                    if (jsonElement != null) {
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

    public int getTotalHits() {
        final JsonElement hits = safeResult().getJsonObject().get("hits");
        if (hits != null) {
            final JsonElement total = hits.getAsJsonObject().get("total");
            if (total != null) {
                return total.getAsInt();
            }
        }
        return 0;
    }

    public List<Result> getHits() {
        List<Result> out = Lists.newArrayList();
        final JsonElement hits = safeResult().getJsonObject().get("hits");
        if (hits != null) {
            final JsonElement hitsList = hits.getAsJsonObject().get("hits");
            if (hitsList != null) {
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
        if (safeResult().isSucceeded()) {
            return new Result(safeResult().getJsonObject()).getObject(clazz);
        }
        return null;
    }

    @Nullable
    public String getId() {
        final JsonObject jsonObject = safeResult().getJsonObject();
        if (jsonObject.has("_id")) {
            return jsonObject.get("_id").getAsString();
        }
        return null;
    }

    @NotNull
    public JestResult getResult() {
        return safeResult();
    }


    public String getPathToResult() {
        return safeResult().getPathToResult();
    }

    public String getJsonString() {
        return safeResult().getJsonString();
    }

    public Object getValue(String key) {
        return safeResult().getValue(key);
    }

    public <T extends Facet> List<T> getFacets(Class<T> type) {
        return safeResult().getFacets(type);
    }

    public boolean isSucceeded() {
        return safeResult().isSucceeded();
    }

    public <T> List<T> getSourceAsObjectList(Class<T> type) {
        return safeResult().getSourceAsObjectList(type);
    }

    public String getErrorMessage() {
        return safeResult().getErrorMessage();
    }

    public JsonObject getJsonObject() {
        return safeResult().getJsonObject();
    }

    public <T> T getSourceAsObject(Class<T> clazz) {
        return safeResult().getSourceAsObject(clazz);
    }

    /**
     * Allows elasticSearch and Gson to talk the same language regarding dates (ISO8601 format)
     */
    static Gson createGsonWithDateFormat() {
        return new GsonBuilder()
                .setDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").create();
    }

    JestResult safeResult() {
        if (result == null) {
            result = new JestResult(gson);
            safeResult().setJsonObject(new JsonObject());
            safeResult().setSucceeded(false);
        }
        return result;
    }

    public static class Result {
        private final String index;
        private final String type;
        private final String id;
        private Float score;
        private final JsonObject result;
        private final JsonObject source;
        private transient Index convertedObject = null;

        public Result(JsonObject resultLine) {
            this.result = resultLine;
            this.index = resultLine.get("_index").getAsString();
            this.type = resultLine.get("_type").getAsString();
            this.id = resultLine.get("_id").getAsString();
            final JsonElement scoreElement = resultLine.get("_score");
            try {
                this.score = resultLine.has("_score") ? scoreElement.getAsFloat() : 1.0f;
            } catch (Exception e) {
                this.score = 1.0f;
            }
            this.source = resultLine.has("_source") ? resultLine.get("_source").getAsJsonObject() : null;
        }

        public String index() {
            return index;
        }

        public String type() {
            return type;
        }

        public String id() {
            return id;
        }

        public Float score() {
            return score;
        }

        public JsonObject result() {
            return result;
        }

        @SuppressWarnings("unchecked")
        public Map<String, Object> sourceAsMap() {
            return gson.fromJson(source, Map.class);
        }

        public JsonObject source() {
            return source;
        }

        @SuppressWarnings("unchecked")
        public <T extends Index> T getObject(Class<T> clazz) {
            if (convertedObject == null) {
                // Get Data Map
                Map<String, Object> map = sourceAsMap();
                // Create a new Indexable Object for the return
                T objectIndexable = IndexUtils.getInstanceIndex(clazz);
                T t = (T) objectIndexable.fromIndex(map);
                t.id = id();
                convertedObject = t;
            }
            return (T) convertedObject;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("source", source)
                    .add("score", score)
                    .add("id", id)
                    .add("type", type)
                    .add("index", index)
                    .toString();
        }
    }
}
