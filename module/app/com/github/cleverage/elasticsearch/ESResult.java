package com.github.cleverage.elasticsearch;

import com.google.common.base.Objects;
import com.google.gson.JsonArray;

import java.util.Map;

/**
 * @author cgatay
 */
public class ESResult {
    public int took;
    public Hits hits;
    
    public static class Hits{
        int total;
        int max_score;
        JsonArray hits;

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("total", total)
                    .add("max_score", max_score)
                    .add("hits", hits)
                    .toString();
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("took", took)
                .add("hits", hits)
                .toString();
    }
}
