package charley.wu.flink.datahub.config;

import charley.wu.flink.datahub.util.ConfigUtil;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.CompressType;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.config.ProducerConfig;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import java.io.Serializable;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
public class DataHubConfig implements Serializable {


    private static final long serialVersionUID = 1L;

    public static final String IDENTIFIER = "datahub";

    public static final ConfigOption<String> ENDPOINT = ConfigOptions.key("endpoint")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> ACCESS_ID = ConfigOptions.key("accessId")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> ACCESS_KEY = ConfigOptions.key("accessKey")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> ENABLE_BINARY = ConfigOptions.key("enableBinary")
            .stringType()
            .noDefaultValue();

    private static final boolean DEFAULT_ENABLE_BINARY = true;

    public static final ConfigOption<String> READ_TIMEOUT = ConfigOptions.key("datahub.http.readTimeout")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> CONN_TIMEOUT = ConfigOptions.key("datahub.http.connTimeout")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> MAX_RETRY_COUNT = ConfigOptions.key("datahub.http.maxRetryCount")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> DEBUG_REQUEST = ConfigOptions.key("datahub.http.debugRequest")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> COMPRESS_TYPE = ConfigOptions.key("datahub.http.compressType")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> PROXY_URI = ConfigOptions.key("datahub.http.proxyUri")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> PROXY_USERNAME = ConfigOptions.key("datahub.http.proxyUsername")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> PROXY_PCODE = ConfigOptions.key("datahub.http.proxyPcode")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> DEFAULT_PROJECT = ConfigOptions.key("datahub.default.project")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> SOURCE_PROJECT = ConfigOptions.key("project")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> SOURCE_TOPIC = ConfigOptions.key("topic")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> SOURCE_SUBID = ConfigOptions.key("subId")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> SINK_PROJECT = ConfigOptions.key("datahub.sink.project")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> SINK_TOPIC = ConfigOptions.key("datahub.sink.topic")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> BATCH_ENABLE = ConfigOptions.key("datahub.sink.batchEnable")
            .stringType()
            .noDefaultValue();
    public static final boolean DEFAULT_BATCH_ENABLE = true;  // 默认批量

    public static final ConfigOption<String> SOURCE_RATE = ConfigOptions.key("datahub.source.rate")
            .stringType()
            .noDefaultValue();

    public static final int DEFAULT_SOURCE_RATE = 50000;  // 默认消费速率
    public static final ConfigOption<String> SINK_RATE = ConfigOptions.key("datahub.sink.rate")
            .stringType()
            .noDefaultValue();

    public static final int DEFAULT_SINK_RATE = 2000;  // 默认生产速率


    private ReadableConfig prop;

    private String defaultProject;
    private String sourceProject;
    private String sinkProject;
    private String sourceTopic;
    private String sinkTopic;
    private String subId;

    public DataHubConfig(ReadableConfig properties) {
        this.prop = properties;

        this.defaultProject = properties.get(DEFAULT_PROJECT);
        if (!Strings.isNullOrEmpty(this.defaultProject)) {
            this.sourceProject = this.defaultProject;
            this.sinkProject = this.defaultProject;
        }

        String configSourceProject = properties.get(SOURCE_PROJECT);
        if (!Strings.isNullOrEmpty(configSourceProject)) {
            this.sourceProject = configSourceProject;
        }

        String configSinkProject = properties.get(SINK_PROJECT);
        if (!Strings.isNullOrEmpty(configSinkProject)) {
            this.sinkProject = configSinkProject;
        }

        this.sourceTopic = this.prop.get(SOURCE_TOPIC);
        this.sinkTopic = this.prop.get(SINK_TOPIC);

        this.subId = this.prop.get(SOURCE_SUBID);
    }

    public DatahubConfig buildDatahubConfig() throws ConfigException {
        String endpoint = prop.get(ENDPOINT);
        if (Strings.isNullOrEmpty(endpoint)) {
            throw new ConfigException("Endpoint can not be null.");
        }

        String accessId = prop.get(ACCESS_ID);
        if (Strings.isNullOrEmpty(accessId)) {
            throw new ConfigException("AccessId can not be null.");
        }

        String accessKey = prop.get(ACCESS_KEY);
        if (Strings.isNullOrEmpty(accessKey)) {
            throw new ConfigException("AccessKey can not be null.");
        }

        boolean enableBinary = ConfigUtil.getBoolean(prop, ENABLE_BINARY, DEFAULT_ENABLE_BINARY);
        AliyunAccount account = new AliyunAccount(accessId, accessKey);
        return new DatahubConfig(endpoint, account, enableBinary);
    }

    public HttpConfig buildHttpConfig() {
        HttpConfig config = new HttpConfig();

        String readTimeout = prop.get(READ_TIMEOUT);
        if (!Strings.isNullOrEmpty(readTimeout)) {
            config.setReadTimeout(Integer.parseInt(readTimeout));
        }

        String connTimeout = prop.get(CONN_TIMEOUT);
        if (!Strings.isNullOrEmpty(connTimeout)) {
            config.setConnTimeout(Integer.parseInt(connTimeout));
        }

        String maxRetryCount = prop.get(MAX_RETRY_COUNT);
        if (!Strings.isNullOrEmpty(maxRetryCount)) {
            config.setMaxRetryCount(Integer.parseInt(maxRetryCount));
        }

        String debugRequest = prop.get(DEBUG_REQUEST);
        if (!Strings.isNullOrEmpty(debugRequest)) {
            config.setDebugRequest(Boolean.parseBoolean(debugRequest));
        }

        String compressType = prop.get(COMPRESS_TYPE);
        if (!Strings.isNullOrEmpty(debugRequest)) {
            config.setCompressType(CompressType.valueOf(compressType));
        }

        String proxyUri = prop.get(PROXY_URI);
        if (!Strings.isNullOrEmpty(proxyUri)) {
            config.setProxyUri(proxyUri);
        }

        String proxyUsername = prop.get(PROXY_USERNAME);
        if (!Strings.isNullOrEmpty(proxyUsername)) {
            config.setProxyUsername(proxyUsername);
        }

        String proxyPassword = prop.get(PROXY_PCODE);
        if (!Strings.isNullOrEmpty(proxyPassword)) {
            config.setProxyPassword(proxyPassword);
        }
        return config;
    }

    public ConsumerConfig buildConsumerConfig() {
        String endpoint = prop.get(ENDPOINT);
        if (Strings.isNullOrEmpty(endpoint)) {
            throw new ConfigException("Endpoint can not be null.");
        }

        String accessId = prop.get(ACCESS_ID);
        if (Strings.isNullOrEmpty(accessId)) {
            throw new ConfigException("AccessId can not be null.");
        }

        String accessKey = prop.get(ACCESS_KEY);
        if (Strings.isNullOrEmpty(accessKey)) {
            throw new ConfigException("AccessKey can not be null.");
        }

        return new ConsumerConfig(endpoint, accessId, accessKey);
    }

    public ProducerConfig buildProducerConfig() {
        String endpoint = prop.get(ENDPOINT);
        if (Strings.isNullOrEmpty(endpoint)) {
            throw new ConfigException("Endpoint can not be null.");
        }

        String accessId = prop.get(ACCESS_ID);
        if (Strings.isNullOrEmpty(accessId)) {
            throw new ConfigException("AccessId can not be null.");
        }

        String accessKey = prop.get(ACCESS_KEY);
        if (Strings.isNullOrEmpty(accessKey)) {
            throw new ConfigException("AccessKey can not be null.");
        }

        return new ProducerConfig(endpoint, accessId, accessKey);
    }

    public String getDefaultProject() {
        return defaultProject;
    }

    public void setDefaultProject(String defaultProject) {
        this.defaultProject = defaultProject;
    }

    public String getSourceProject() {
        return sourceProject;
    }

    public void setSourceProject(String sourceProject) {
        this.sourceProject = sourceProject;
    }

    public String getSinkProject() {
        return sinkProject;
    }

    public void setSinkProject(String sinkProject) {
        this.sinkProject = sinkProject;
    }

    public String getSourceTopic() {
        if (sourceTopic == null) {
            throw new ConfigException("Source topic can not be null.");
        }
        return sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public String getSinkTopic() {
        if (sinkTopic == null) {
            throw new ConfigException("Sink topic can not be null.");
        }
        return sinkTopic;
    }

    public void setSinkTopic(String sinkTopic) {
        this.sinkTopic = sinkTopic;
    }

    public String getSubId() {
        if (subId == null) {
            throw new ConfigException("SubId can not be null.");
        }
        return subId;
    }

    public void setSubId(String subId) {
        this.subId = subId;
    }
}
