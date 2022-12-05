package com.jdcloud.logs.producer.core;

import com.jdcloud.logs.api.util.StringUtils;

/**
 * 分组
 *
 * @author liubai
 * @date 2022/11/21
 */
public class GroupKey {

    private static final String DELIMITER = "|";
    private static final String DEFAULT_VALUE = "_";
    private final String key;
    private final String regionId;
    private final String logTopic;
    private final String source;
    private final String fileName;

    public GroupKey(String regionId, String logTopic, String source, String fileName) {
        this.regionId = regionId;
        this.logTopic = logTopic;
        this.source = source;
        this.fileName = fileName;
        this.key = getOrDefault(regionId) + DELIMITER
                + getOrDefault(logTopic) + DELIMITER
                + getOrDefault(source) + DELIMITER
                + getOrDefault(fileName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GroupKey groupKey = (GroupKey) o;

        return key.equals(groupKey.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return key;
    }

    public String getKey() {
        return key;
    }

    public String getRegionId() {
        return regionId;
    }

    public String getLogTopic() {
        return logTopic;
    }

    public String getSource() {
        return source;
    }

    public String getFileName() {
        return fileName;
    }

    private String getOrDefault(String value) {
        return StringUtils.defaultIfBlank(value, DEFAULT_VALUE);
    }
}
