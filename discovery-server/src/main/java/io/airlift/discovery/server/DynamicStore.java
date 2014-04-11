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

import java.util.Set;

public interface DynamicStore
{
    void put(Id<Node> nodeId, DynamicAnnouncement announcement);
    void delete(Id<Node> nodeId);
    void delete(Id<Node> nodeId, String applicationType);

    Set<Service> getAll();
    Set<Service> get(String type);
    Set<Service> get(String type, String pool);
}
