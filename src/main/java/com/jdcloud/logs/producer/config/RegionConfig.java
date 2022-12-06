package com.jdcloud.logs.producer.config;

public class RegionConfig {

    /**
     * 认证标识
     */
    private final String accessKeyId;

    /**
     * 认证秘钥
     */
    private final String secretAccessKey;

    /**
     * 地域标识
     */
    private final String regionId;

    /**
     * 日志发送地址
     */
    private final String endpoint;

    public RegionConfig(String accessKeyId, String secretAccessKey, String regionId, String endpoint) {
        if (accessKeyId == null) {
            throw new NullPointerException("accessKeyId cannot be null");
        }
        if (secretAccessKey == null) {
            throw new NullPointerException("secretAccessKey cannot be null");
        }
        if (regionId == null) {
            throw new NullPointerException("regionId cannot be null");
        }
        if (endpoint == null) {
            throw new NullPointerException("endpoint cannot be null");
        }
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.regionId = regionId;
        this.endpoint = endpoint;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public String getRegionId() {
        return regionId;
    }

    public String getEndpoint() {
        return endpoint;
    }

}
