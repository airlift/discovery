package com.proofpoint.discovery;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Row;
import org.testng.annotations.Test;

import java.util.NoSuchElementException;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Iterables.skip;
import static com.proofpoint.discovery.CassandraPaginator.paginate;
import static com.proofpoint.discovery.TestCassandraPaginator.TestRow.from;
import static org.testng.Assert.assertEquals;

public class TestCassandraPaginator
{
    @Test
    public void testEmpty()
    {
        assertEquals(copyOf(paginate(queryForList(ImmutableList.<TestRow>of()), 0, 4)), ImmutableList.of());
    }

    @Test
    public void testOne()
    {
        final ImmutableList<TestRow> expected = ImmutableList.of(from(0));
        assertEquals(copyOf(paginate(queryForList(expected), 0, 4)), expected);
    }

    @Test
    public void testOneFullPage()
    {
        final ImmutableList<TestRow> expected = ImmutableList.of(from(0), from(1), from(2), from(3));
        assertEquals(copyOf(paginate(queryForList(expected), 0, 4)), expected);
    }

    @Test
    public void testMoreThanOnePageWithPartialLastPage()
    {
        final ImmutableList<TestRow> expected = ImmutableList.of(from(0), from(1), from(2), from(3), from(4), from(5));
        assertEquals(copyOf(paginate(queryForList(expected), 0, 4)), expected);
    }

    @Test
    public void testMoreThanOnePageWithFullLastPage()
    {
        final ImmutableList<TestRow> expected = ImmutableList.of(from(0), from(1), from(2), from(3), from(4), from(5), from(6), from(7));
        assertEquals(copyOf(paginate(queryForList(expected), 0, 4)), expected);
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testThrows()
    {
        paginate(queryForList(ImmutableList.<TestRow>of()), 0, 4).iterator().next();
    }

    private CassandraPaginator.PageQuery<Integer, Integer, Integer> queryForList(final ImmutableList<TestRow> expected)
    {
        return new CassandraPaginator.PageQuery<Integer, Integer, Integer>()
        {
            @Override
            public Iterable<? extends Row<Integer, Integer, Integer>> query(Integer start, int count)
            {
                return limit(skip(expected, start), count);
            }
        };
    }

    public static class TestRow
            implements Row<Integer, Integer, Integer>
    {
        private final Integer key;

        public static TestRow from(int key)
        {
            return new TestRow(key);
        }

        private TestRow(Integer key)
        {
            Preconditions.checkNotNull(key, "key is null");
            this.key = key;
        }

        @Override
        public Integer getKey()
        {
            return key;
        }

        @Override
        public ColumnSlice<Integer, Integer> getColumnSlice()
        {
            throw new UnsupportedOperationException();
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

            TestRow testRow = (TestRow) o;

            if (!key.equals(testRow.key)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return key.hashCode();
        }
    }
}
