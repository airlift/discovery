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
package io.airlift.discovery.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.annotation.concurrent.Immutable;

import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

@Immutable
public final class Id<T>
{
    private final String id;

    @JsonCreator
    public static <T> Id<T> valueOf(String id)
    {
        return new Id<>(id);
    }

    public static <T> Id<T> random()
    {
        return new Id<>(UUID.randomUUID().toString());
    }

    private Id(String id)
    {
        checkNotNull(id, "id is null");
        checkArgument(!id.isEmpty(), "id is empty");
        this.id = id;
    }

    @JsonValue
    public String get()
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
        return id.equals(((Id<?>) o).id);
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public String toString()
    {
        return id;
    }

    public byte[] getBytes()
    {
        return toString().getBytes(UTF_8);
    }
}
