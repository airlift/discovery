package com.proofpoint.discovery.store;

import com.google.common.base.Preconditions;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.Arrays;

@Immutable
public class Entry
{
    private final byte[] key;
    private final byte[] value;
    private final Version version;
    private final long timestamp;
    private final Long maxAgeInMs;

    public Entry(@JsonProperty("key") byte[] key,
            @JsonProperty("value") byte[] value,
            @JsonProperty("version") Version version, 
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("maxAge") Long maxAgeInMs)
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(version, "version is null");
        Preconditions.checkArgument(maxAgeInMs == null || maxAgeInMs > 0, "maxAgeInMs must be greater than 0");

        this.value = value;
        this.key = key;
        this.timestamp = timestamp;
        this.maxAgeInMs = maxAgeInMs;
        this.version = version;
    }

    @JsonProperty
    public byte[] getKey()
    {
        return key;
    }

    @JsonProperty
    public byte[] getValue()
    {
        return value;
    }

    @JsonProperty
    public Version getVersion()
    {
        return version;
    }

    @JsonProperty
    public long getTimestamp()
    {
        return timestamp;
    }

    @JsonProperty
    public Long getMaxAgeInMs()
    {
        return maxAgeInMs;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Entry entry = (Entry) o;

        if (timestamp != entry.timestamp) {
            return false;
        }
        if (!Arrays.equals(key, entry.key)) {
            return false;
        }
        if (maxAgeInMs != null ? !maxAgeInMs.equals(entry.maxAgeInMs) : entry.maxAgeInMs != null) {
            return false;
        }
        if (!Arrays.equals(value, entry.value)) {
            return false;
        }
        if (!version.equals(entry.version)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = Arrays.hashCode(key);
        result = 31 * result + (value != null ? Arrays.hashCode(value) : 0);
        result = 31 * result + version.hashCode();
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (maxAgeInMs != null ? maxAgeInMs.hashCode() : 0);
        return result;
    }
}
