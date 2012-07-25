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
package io.airlift.discovery;

import com.google.common.base.Preconditions;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

import javax.annotation.concurrent.Immutable;
import java.util.UUID;

@Immutable
public class Id<T>
{
    private final UUID id;

    @JsonCreator
    public static <T> Id<T> valueOf(UUID id)
    {
        Preconditions.checkNotNull(id, "id is null");
        return new Id<T>(id);
    }

    public static <T> Id<T> valueOf(String id)
    {
        Preconditions.checkNotNull(id, "id is null");
        return new Id<T>(UUID.fromString(id));
    }

    public static <T> Id<T> random()
    {
        return new Id<T>(UUID.randomUUID());
    }

    private Id(UUID id)
    {
        this.id = id;
    }

    @JsonValue
    public UUID get()
    {
        return id;
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

        Id id1 = (Id) o;

        if (!id.equals(id1.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public String toString()
    {
        return id.toString();
    }
}
