package com.proofpoint.discovery;

public class TestInMemoryStaticStore
    extends TestStaticStore
{
    @Override
    protected StaticStore initializeStore()
    {
        return new InMemoryStaticStore();
    }
}
