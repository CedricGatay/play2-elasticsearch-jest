package com.codetroopers.play.elasticsearch.plugin;

import org.elasticsearch.client.transport.NoNodeAvailableException;

import play.Application;
import play.Logger;
import play.Plugin;

import com.codetroopers.play.elasticsearch.IndexClient;
import com.codetroopers.play.elasticsearch.IndexService;

/**
 * ElasticSearch PLugin for Play 2 written in Java.
 * User: nboire
 * Date: 12/05/12
 */
public class IndexPlugin extends Plugin {
    private final Application application;

    public IndexPlugin(Application application) {
        this.application = application;
    }

    private boolean isPluginDisabled() {
        String status = application.configuration().getString("elasticsearch.plugin");
        return status != null && status.equals("disabled");
    }

    @Override
    public boolean enabled() {
        return !isPluginDisabled();
    }

    @Override
    public void onStart() {
        // ElasticSearch client start on local or network
        new IndexClient(application);

        // Load indexName, indexType, indexMapping from annotation
        IndexClient.config.loadFromAnnotations();

        try {
            IndexClient.start();
        } catch (Exception e) {
            Logger.error("ElasticSearch : Error when starting ElasticSearch Client ", e);
        }

        // We catch these exceptions to allow application to start even if the module start fails
        try {
            // Create Indexs and Mappings if not Exists
            String[] indexNames = IndexClient.config.indexNames;
            for (String indexName : indexNames) {

                if (!IndexService.existsIndex(indexName)) {
                    // Create index
                    IndexService.createIndex(indexName);

                    // Prepare Index ( define mapping if present )
                    IndexService.prepareIndex(indexName);
                }
            }

            // Create "_percolator" index if not exists
            if (!IndexService.existsIndex(IndexService.TYPE_NAME_PERCOLATOR)) {
                IndexService.createIndex(IndexService.TYPE_NAME_PERCOLATOR);
            }

            Logger.info("ElasticSearch : Plugin has started");

        } catch (NoNodeAvailableException e) {
            Logger.error("ElasticSearch : No ElasticSearch node is available. Please check that your configuration is " +
                    "correct, that you ES server is up and reachable from the network. Index has not been created and prepared.", e);
        } catch (Exception e) {
            Logger.error("ElasticSearch : An unexpected exception has occurred during index preparation. Index has not been created and prepared.", e);
        }

    }

    @Override
    public void onStop() {
        // Deleting index(s) if define in conf
        if (IndexClient.config != null && IndexClient.config.dropOnShutdown) {
            String[] indexNames = IndexClient.config.indexNames;
            for (String indexName : indexNames) {
                if (IndexService.existsIndex(indexName)) {
                    IndexService.deleteIndex(indexName);
                }
            }
        }

        // Stopping the client
        try {
            IndexClient.stop();
        } catch (Exception e) {
            Logger.error("ElasticSearch : error when stop plugin ", e);
        }
        Logger.info("ElasticSearch : Plugin has stopped");
    }
}
