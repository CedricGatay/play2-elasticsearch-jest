package com.github.cleverage.elasticsearch.jest;

import com.github.cleverage.elasticsearch.AsyncUtils;
import com.github.cleverage.elasticsearch.IndexClient;
import io.searchbox.Action;
import io.searchbox.client.JestResult;
import play.libs.F;

import javax.annotation.Nullable;

/**
 * @author cgatay
 */
public abstract class JestRequest<T extends Action> {
    public abstract T getAction();
    
    @Nullable
    public JestResult execute(){
        return JestClientWrapper.execute(this);
    }

    public F.Promise<JestResult> executeAsync(){
        return F.Promise.wrap(AsyncUtils.executeAsync(IndexClient.client, getAction()));
    }
}
