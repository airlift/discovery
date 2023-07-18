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
import com.google.common.primitives.Longs;

import com.google.errorprone.annotations.Immutable;

@Immutable
public class Version
{
    private final long sequence;

    @JsonCreator
    public Version(@JsonProperty("sequence") long sequence)
    {
        this.sequence = sequence;
    }

    @JsonProperty
    public long getSequence()
    {
        return sequence;
    }

    public Occurs compare(Version other)
    {
        int comparison = Longs.compare(sequence, other.sequence);

        if (comparison < 0) {
            return Occurs.BEFORE;
        }
        if (comparison > 0) {
            return Occurs.AFTER;
        }

        return Occurs.SAME;
    }

    public enum Occurs
    {
        BEFORE, SAME, CONCURRENT, AFTER
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

        Version version = (Version) o;

        return sequence == version.sequence;
    }

    @Override
    public int hashCode()
    {
        return (int) (sequence ^ (sequence >>> 32));
    }
}
