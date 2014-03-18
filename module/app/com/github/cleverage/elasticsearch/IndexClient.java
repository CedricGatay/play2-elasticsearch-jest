package com.github.cleverage.elasticsearch;

import com.github.cleverage.elasticsearch.jest.JestClientWrapper;
import io.searchbox.client.JestClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.NodeBuilder;
import play.Application;
import play.Logger;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class IndexClient {

    public static org.elasticsearch.node.Node node = null;

    public static JestClient client = null;

    public static IndexConfig config;

    public IndexClient(Application application) {
        config = new IndexConfig(application);
    }

    public static void start() throws Exception {
        ImmutableSettings.Builder settings = config.loadSettings();
        if (config.isLocalMode()) {
            Logger.info("ElasticSearch : Starting in Local Mode");

            NodeBuilder nb = nodeBuilder().settings(settings).local(true).client(false).data(true);
            node = nb.node();
            Logger.info("ElasticSearch : Started in Local Mode");
        }

        client = JestClientWrapper.jestClient(config);

        // Check Client
        if (client == null) {
            throw new Exception("ElasticSearch Client cannot be null - please check the configuration provided and the health of your ElasticSearch instances.");
        }
    }


    public static void stop() throws Exception {
        if (client != null) {
            client.shutdownClient();
        }
        if (node != null) {
            node.close();
        }
    }
}
