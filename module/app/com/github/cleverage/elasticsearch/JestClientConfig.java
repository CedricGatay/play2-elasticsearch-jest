package com.github.cleverage.elasticsearch;


import com.fasterxml.jackson.databind.ObjectMapper;
import io.searchbox.AbstractAction;
import io.searchbox.AbstractMultiIndexActionBuilder;
import io.searchbox.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.indices.Flush;
import play.libs.F;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JestClientConfig  {
    public static JestClient jestClient() throws Exception {

        String connectionUrl;

        if (System.getenv("SEARCHBOX_URL") != null) {
            // Heroku
            connectionUrl = System.getenv("SEARCHBOX_URL");

        } else if (System.getenv("VCAP_SERVICES") != null) {
            // CloudFoundry
            Map result = new ObjectMapper().readValue(System.getenv("VCAP_SERVICES"), HashMap.class);
            connectionUrl = (String) ((Map) ((Map) ((List)
                    result.get("searchly-n/a")).get(0)).get("credentials")).get("uri");
        } else {
            // generic or CloudBees
//            connectionUrl = "https://t2a6vxdu:dsb9486p3b1v6m7w@cypress-7521189.eu-west-1.bonsai.io/";//http://site:your-api-key@api.searchbox.io";
            connectionUrl = "http://localhost:9200";
        }

        // Configuration
        HttpClientConfig clientConfig = new HttpClientConfig.Builder(connectionUrl).multiThreaded(true).build();

        // Construct a new Jest client according to configuration via factory
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(clientConfig);
        return factory.getObject();
    }

    @Nullable
    public static JestResult jestXcute(final Action action){
        try {
            return IndexClient.client.execute(action);
        } catch (Exception e) {
            e.printStackTrace();
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
