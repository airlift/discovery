package com.proofpoint.discovery;

import com.google.common.base.Predicate;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;

public class ColumnFamilies
{
    public static Predicate<ColumnFamilyDefinition> named(final String name)
    {
        return new Predicate<ColumnFamilyDefinition>()
        {
            public boolean apply(ColumnFamilyDefinition input)
            {
                return input.getName().equals(name);
            }
        };
    }
}
