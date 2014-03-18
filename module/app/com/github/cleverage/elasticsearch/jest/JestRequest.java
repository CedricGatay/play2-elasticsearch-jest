package com.github.cleverage.elasticsearch.jest;

import io.searchbox.Action;

/**
 * @author cgatay
 */
public interface JestRequest<T extends Action> {
    T getAction();
}