package com.codetroopers.play.elasticsearch.jest;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.searchbox.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

/**
 * @author cgatay
 */
public class JestClientCredentialsTest {

    private JestClient jestClient;

    @Before
    public void setUp() throws Exception {
        //endpoint matching user/passwd against the next two path arguments basic-auth/cgatay/test is valid
        jestClient = JestClientWrapper.buildJestClient(Arrays.asList("http://cgatay:test@httpbin.org/basic-auth/"));
    }

    @After
    public void tearDown() throws Exception {
        if (jestClient != null){
            jestClient.shutdownClient();
        }
    }

    @Test
    public void testBasicAuth_ValidCredentials() throws Exception {
        final Action action = new GetRestAction("cgatay/test");
        final JestResult jestResult = jestClient.execute(action);
        Assert.assertTrue("Credentials should be valid",jestResult.isSucceeded());
    }

    @Test
    public void testBasicAuth_InvalidCredentials() throws Exception {
        final Action action = new GetRestAction("cgatay/invalid");
        final JestResult jestResult = jestClient.execute(action);
        Assert.assertFalse("Credentials should not be valid", jestResult.isSucceeded());
    }

    private static class GetRestAction implements Action {
        private String uriToCall;

        private GetRestAction(String uriToCall) {
            this.uriToCall = uriToCall;
        }

        @Override
        public String getURI() {
            return uriToCall;
        }

        @Override
        public String getRestMethodName() {
            return "GET";
        }

        @Override
        public Object getData(Gson gson) {
            return null;
        }

        @Override
        public String getPathToResult() {
            return null;
        }

        @Override
        public Map<String, Object> getHeaders() {
            return Maps.newHashMap();
        }

        @Override
        public Boolean isOperationSucceed(Map<String, ?> result) {
            return true;
        }

        @Override
        public Boolean isOperationSucceed(JsonObject result) {
            return true;
        }
    }
}
