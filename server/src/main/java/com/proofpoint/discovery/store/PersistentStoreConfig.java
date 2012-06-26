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
package com.proofpoint.discovery.store;

import com.proofpoint.configuration.Config;

import javax.validation.constraints.NotNull;
import java.io.File;

public class PersistentStoreConfig
{
    private File location = new File("db");

    @NotNull
    public File getLocation()
    {
        return location;
    }

    @Config("db.location")
    public PersistentStoreConfig setLocation(File location)
    {
        this.location = location;
        return this;
    }
}
