package com.github.cleverage.elasticsearch;

import io.searchbox.client.JestResult;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;

import javax.annotation.Nullable;

/**
 * @author cgatay
 */
class JestGetRequestBuilder {
    private final String index;
    private final String type;
    private final String id;

    public JestGetRequestBuilder(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    @Nullable
    public JestResult jestXcute(){
        final Get build = getAction();

        try {
            return IndexClient.client.execute(build);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Get getAction() {
        return new Get.Builder(index, id).type(type).build();
    }
}
