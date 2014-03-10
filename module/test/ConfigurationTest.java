import com.github.cleverage.elasticsearch.IndexClient;
import com.google.common.collect.Maps;
import org.junit.Test;
import play.Play;

import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;


public class ConfigurationTest {


    @Test
    public void checkClientConfigValidation() {
        Map<String, Object> configuration = Maps.newHashMap();
        configuration.put("elasticsearch.local", "false");
        configuration.put("elasticsearch.client", "127.0.0.1:2002");

        testWorkingCase(configuration);
    }

    @Test
    public void checkMultipleClientConfigValidation() {
        Map<String, Object> configuration = Maps.newHashMap();
        configuration.put("elasticsearch.local", "false");
        configuration.put("elasticsearch.client", "127.0.0.1:2002,127.0.0.1:2300");

        testWorkingCase(configuration);
    }

    @Test
    public void checkClientConfigValidationWithError() {
        Map<String, Object> configuration = Maps.newHashMap();
        configuration.put("elasticsearch.local", "false");
        configuration.put("elasticsearch.client", "127.0.0.1;2002,127.0.0.1:2300");

        testThrowingExceptionCase(configuration, "Invalid Host: http://127.0.0.1;2002");
    }

    @Test
    public void checkClientConfigValidationWithProtocol() {
        Map<String, Object> configuration = Maps.newHashMap();
        configuration.put("elasticsearch.local", "false");
        configuration.put("elasticsearch.client", "http://127.0.0.1");

        testWorkingCase(configuration);
    }

    @Test
    public void checkClientConfigValidationWithProtocolAndPort() {
        Map<String, Object> configuration = Maps.newHashMap();
        configuration.put("elasticsearch.local", "false");
        configuration.put("elasticsearch.client", "http://127.0.0.1:2000");

        testWorkingCase(configuration);
    }

    @Test
    public void checkClientConfigValidationWithCredential() {
        Map<String, Object> configuration = Maps.newHashMap();
        configuration.put("elasticsearch.local", "false");
        configuration.put("elasticsearch.client", "http://test:test@127.0.0.1");

        testWorkingCase(configuration);
    }

    @Test
    public void checkClientConfigValidationWithProtocolCredentialAndPort() {
        Map<String, Object> configuration = Maps.newHashMap();
        configuration.put("elasticsearch.local", "false");
        configuration.put("elasticsearch.client", "http://test:test@127.0.0.1:3600");

        testWorkingCase(configuration);
    }

    @Test
    public void checkClientConfigValidationWithCredentialAndPort() {
        Map<String, Object> configuration = Maps.newHashMap();
        configuration.put("elasticsearch.local", "false");
        configuration.put("elasticsearch.client", "user:password@127.0.0.1:80");

        testWorkingCase(configuration);
    }



    private void testWorkingCase(Map<String, Object> configuration) {
        running(fakeApplication(configuration), new Runnable() {
            public void run() {
                IndexClient indexClient = new IndexClient(Play.application());
                try {
                    indexClient.start();
                    assertThat(indexClient.client).isNotNull();
                } catch (Exception e) {
                    fail("elasticsearch.client config param is correct, exception should not have been thrown  : ", e);
                }
            }
        });
    }

    private void testThrowingExceptionCase(Map<String, Object> configuration, final String expectedExceptionMessage) {
        running(fakeApplication(configuration), new Runnable() {
            public void run() {
                IndexClient indexClient = new IndexClient(Play.application());
                try {
                    indexClient.start();
                    fail("elasticsearch.client is not correct, an exception should have been thrown");
                } catch (Exception e) {
                    assertThat(e).hasMessage(expectedExceptionMessage);
                }
            }
        });
    }
}
