package com.proofpoint.discovery;

import org.testng.annotations.BeforeMethod;

public class TestInMemoryStore
    extends TestStore
{
    @BeforeMethod
    public void setup()
    {
        store = new InMemoryStore(new DiscoveryConfig());
    }
}
