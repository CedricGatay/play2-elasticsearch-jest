package com.github.cleverage.elasticsearch;

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

    public static org.elasticsearch.client.Client client = null;

    public static IndexConfig config;

    public IndexClient(Application application) {
        // ElasticSearch config load from application.conf
        this.config = new IndexConfig(application);
    }

    public void start() throws Exception {

        // Load Elasticsearch Settings
        ImmutableSettings.Builder settings = loadSettings();

        // Check Model
        if (this.isLocalMode()) {
            Logger.info("ElasticSearch : Starting in Local Mode");

            NodeBuilder nb = nodeBuilder().settings(settings).local(true).client(false).data(true);
            node = nb.node();
            client = node.client();
            Logger.info("ElasticSearch : Started in Local Mode");
        } else {
            Logger.info("ElasticSearch : Starting in Client Mode");
            client = configureTransportClient(settings);
            Logger.info("ElasticSearch : Started in Client Mode");
        }

        // Check Client
        if (client == null) {
            throw new Exception("ElasticSearch Client cannot be null - please check the configuration provided and the health of your ElasticSearch instances.");
        }
    }


    private TransportClient configureTransportClient(ImmutableSettings.Builder settings) throws Exception {
        TransportClient c = new TransportClient(settings);
        if (config.client == null) {
            throw new Exception("Configuration required - elasticsearch.client when local model is disabled!");
        }

        String[] hosts = config.client.trim().split(",");
        boolean done = false;
        for (String host : hosts) {
            //check if credential are contained in param
            long time = System.currentTimeMillis();
            try {
                boolean protocolSetted = false;
                //URL need a protocol
                if(!host.contains("://")){
                    //set default protocol
                    host = "http://" + host;
                    protocolSetted = true;
                }

                URL url = new URL(host);

                String hostValue = url.getHost();
                //adding credentials if exist
                if (url.getUserInfo() != null) {
                    hostValue = url.getUserInfo() + "@" + hostValue;
                }

                int port = url.getPort();
                if (port == -1 && url.getProtocol().equals("http") && !protocolSetted) {
                    port = 80;
                }

                if(port == -1){
                    throw new Exception("Invalid Host: " + host);
                }

                Logger.info("ElasticSearch : Client - Host: " + hostValue + " Port: " + port);
                c.addTransportAddress(new InetSocketTransportAddress(hostValue, port));
                done = true;
            } catch (Exception e) {
                throw new Exception("Invalid Host: " + host);
            }
        }

        if (!done) {
            throw new Exception("No Hosts Provided for ElasticSearch!");
        }
        return c;
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
            client.close();
        }
        if (node != null) {
            node.close();
        }
    }
}
