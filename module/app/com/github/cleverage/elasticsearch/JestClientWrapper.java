package com.github.cleverage.elasticsearch;


import com.google.common.collect.Lists;
import io.searchbox.AbstractMultiIndexActionBuilder;
import io.searchbox.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import play.Logger;
import play.libs.F;

import javax.annotation.Nullable;
import java.util.List;

import static io.searchbox.client.config.HttpClientConfig.Builder;

public class JestClientWrapper {
    public static JestClient jestClient(IndexConfig config) throws Exception {

        final List<String> connectionUrls = Lists.newArrayList();
        if (config.isLocalMode()){
            connectionUrls.add("http://localhost:9200");
        }else{
            final String[] urlChunks = config.client.split(",");
            for (String urlChunk : urlChunks) {
                if (!urlChunk.startsWith("http")){
                    connectionUrls.add("http://" + urlChunk);      
                }else{
                    connectionUrls.add(urlChunk);
                }
            }
        }
        final Builder builder = new Builder(connectionUrls)
                .multiThreaded(true);
        
        builder.gson(JestResultUtils.createGsonWithDateFormat());
        HttpClientConfig clientConfig = builder.build();

        // Construct a new Jest client according to configuration via factory
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(clientConfig);
        return factory.getObject();
    }

    @Nullable
    public static JestResult jestXcute(final JestRequest jestRequest){
        return jestXcute(jestRequest.getAction());
    }

    @Nullable
    public static JestResult jestXcute(final Action action){
        try {
            return IndexClient.client.execute(action);
        } catch (Exception e) {
            Logger.error("ElasticSearch : Unable to execute request {}", e);
        }
        return null;
    }
    
    
    public static F.Promise<JestResult> jestXcuteAsync(final Action action){
        return F.Promise.wrap(AsyncUtils.executeAsync(IndexClient.client, action));
    }

    public static JestResult jestXcute(AbstractMultiIndexActionBuilder builder) {
        return jestXcute(builder.build());
    }
}
