package com.github.cleverage.elasticsearch;

import io.searchbox.client.JestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.NodeBuilder;
import play.Application;
import play.Logger;

import java.net.URL;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class IndexClient {

    public static org.elasticsearch.node.Node node = null;

    public static JestClient client = null;

    public static IndexConfig config;

    public IndexClient(Application application) {
        // ElasticSearch config load from application.conf
        this.config = new IndexConfig(application);
    }

    public void start() throws Exception {
        // Load Elasticsearch Settings
        ImmutableSettings.Builder settings = loadSettings();
        if (this.isLocalMode()) {
            Logger.info("ElasticSearch : Starting in Local Mode");

            NodeBuilder nb = nodeBuilder().settings(settings).local(true).client(false).data(true);
            node = nb.node();
            Logger.info("ElasticSearch : Started in Local Mode");
        }

        client = JestClientConfig.jestClient();

        // Check Client
        if (client == null) {
            throw new Exception("ElasticSearch Client cannot be null - please check the configuration provided and the health of your ElasticSearch instances.");
        }
    }


    /**
     * Checks if is local mode.
     *
     * @return true, if is local mode
     */
    private boolean isLocalMode() {
        try {
            if (config.client == null) {
                return true;
            }
            if (config.client.equalsIgnoreCase("false") || config.client.equalsIgnoreCase("true")) {
                return true;
            }

            return config.local;
        } catch (Exception e) {
            Logger.error("Error! Starting in Local Model: %s", e);
            return true;
        }
    }

    /**
     * Load settings from resource file
     *
     * @return
     * @throws Exception
     */
    private ImmutableSettings.Builder loadSettings() throws Exception {
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();

        // set default settings
        settings.put("client.transport.sniff", config.sniffing);
        if (config.clusterName != null) {
            settings.put("cluster.name", config.clusterName);
        }

        // load settings
        if (config.localConfig != null) {
            Logger.debug("Elasticsearch : Load settings from " + config.localConfig);
            try {
                settings.loadFromClasspath(config.localConfig);
            } catch (SettingsException settingsException) {
                Logger.error("Elasticsearch : Error when loading settings from " + config.localConfig);
                throw new Exception(settingsException);
            }
        }
        settings.build();
        Logger.info("Elasticsearch : Settings  " + settings.internalMap().toString());
        return settings;
    }

    public void stop() throws Exception {
        if (client != null) {
            client.shutdownClient();
        }
        if (node != null) {
            node.close();
        }
    }
}
