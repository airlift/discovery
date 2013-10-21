/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.discovery.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Entry
{
    private final byte[] key;
    private final byte[] value;
    private final long timestamp;
    private final Long maxAgeInMs;

    @JsonCreator
    public Entry(@JsonProperty("key") byte[] key,
            @JsonProperty("value") byte[] value,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("maxAge") Long maxAgeInMs)
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkArgument(maxAgeInMs == null || maxAgeInMs > 0, "maxAgeInMs must be greater than 0");

        this.value = value;
        this.key = key;
        this.timestamp = timestamp;
        this.maxAgeInMs = maxAgeInMs;
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

    @Deprecated
    @JsonProperty
    public Version getVersion()
    {
        return new Version(timestamp);
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
    public int hashCode()
    {
        return Objects.hashCode(key, value, timestamp, maxAgeInMs);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Entry other = (Entry) obj;
        return Objects.equal(this.key, other.key) && Objects.equal(this.value, other.value) && Objects.equal(this.timestamp, other.timestamp) && Objects.equal(this.maxAgeInMs, other.maxAgeInMs);
    }
}
