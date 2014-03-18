# play2-elasticsearch-jest
===================

This module provides an easy [Elasticsearch](http://www.elasticsearch.org/) integration in a [Playframework](http://www.playframework.com/) 2 application

This module relies on [Jest](https://github.com/searchbox-io/Jest) integration to allow querying of ElasticSearch index over HTTP through its REST api. It is heavily based on [play2-elasticsearch from clever-age](https://github.com/cleverage/play2-elasticsearch) (thanks for the work !).

While classes can look similar to the one in the original plugin, a lot of method signatures have changed due to the use of another querying library. So it is **not** a simple drop-in replacement !

## Reason for this
We needed a way to use ElasticSearch on different machines / network than the one we are running our applications on. So the only way of doing this "securely" is to use REST with authentication which is handled by Jest library.

## Major changes
Almost every operation returns a `JestRichResult` which provides a way to encapsulate common operations (such as `getId()`, `getHits()`...). `JestRichResult` features a _safe_ delegate to `JestResult` allowing to manipulate this object as if it were a `JestResult`.

As the library now uses HTTP transport, expect it to be slower than its counterpart using the Transport protocol, however, it should be more robust when it comes to ElasticSearch version support (as the REST api is more stable than binary compatibilty).

Everything has been relocated to the base package com.codetroopers.play.elasticsearch

## Versions
** 0.1.0 ** [2014-03-18] : Initial release

## Install

The dependency declaration is :
```
"com.code-troopers.play" % "play2-elasticsearch-jest" % "0.1.0"
```

Releases are published on maven-central, so you don't have to define any specific resolvers.

## Activate the plugin

The Play2-elasticsearch module requires its plugin class to be declared in the `conf/play.plugins` file. If this file doesn't exist (it's not created by default when you create a new project),
just create it in the conf directory first, and then add
```
9000:com.codetroopers.play.elasticsearch.plugin.IndexPlugin
```

## Configuration
You can configure the module in conf/application.conf (or in any configuration file included in your application.conf)

```
## ElasticSearch Configuration
##############################
## define local mode or not
elasticsearch.local=false

## Coma-separated list of clients
elasticsearch.client="192.168.0.46:9200"
# ex : elasticsearch.client="192.168.0.46:9200,http://me:pwd@myserver.tld:9200"

## Name of the index
elasticsearch.index.name="play2-elasticsearch"

## Custom settings to apply when creating the index (optional)
elasticsearch.index.settings="{ analysis: { analyzer: { my_analyzer: { type: \"custom\", tokenizer: \"standard\" } } } }"

## define package or class separate by commas for loading @IndexType and @IndexMapping information
elasticsearch.index.clazzs="indexing.*"

## show request & result json of search request in log (it will be logged using Logger.debug())
elasticsearch.index.show_request=true
```

## Usage
There is two ways of using this plugin : 
 * call `IndexQuery.*` or `IndexService.*` methods
 * create your own Jest `Action` through the correct `Builder` (for example : `new DeleteByQuery.Builder()`) and then call `JestClientWrapper.execute(yourAction)`

## What needs to be done
 * A lot of improvements in the API can be done (needs to find out whether Jest*RequestBuilder are useful or not)
 * Add better return types (`JestRichResult` is incomplete) 
 * Percolate API is missing in `IndexService` (accessible via Jest `Builder`)
 * A lot of tests are missing

## Authors
http://twitter.com/Cedric_Gatay

Original work from http://twitter.com/nboire & http://twitter.com/mguillermin

## License
This code is released under the MIT License