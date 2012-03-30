package com.proofpoint.discovery.store;

import com.google.common.primitives.Longs;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;

@Immutable
class Version
{
    private final long sequence;

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
        else if (comparison > 0) {
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

        if (sequence != version.sequence) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (sequence ^ (sequence >>> 32));
    }
}
