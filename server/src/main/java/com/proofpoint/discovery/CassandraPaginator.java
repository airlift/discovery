package com.proofpoint.discovery;

import com.google.common.collect.Iterators;
import me.prettyprint.hector.api.beans.Row;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class CassandraPaginator
{
    public static <K, N, V> Iterable<? extends Row<K, N, V>> paginate(final PageQuery<K, N, V> query, @Nullable final K start, final int pageSize)
    {
        return new Iterable<Row<K, N, V>>() {
            public Iterator<Row<K, N, V>> iterator()
            {
                return new QueryIterator<K, N, V>(start, query, pageSize);
            }
        };
    }

    public static interface PageQuery<K, N, V>
    {
        Iterable<? extends Row<K, N, V>> query(K start, int count);
    }

    private static class QueryIterator<K, N, V>
            implements Iterator<Row<K, N, V>>
    {
        private Iterator<? extends Row<K, N, V>> currentPage;
        private boolean lastPage;
        private boolean firstPage = true;
        private Row<K, N, V> next;
        private K start;
        private int count;

        private final PageQuery<K, N, V> query;
        private final int pageSize;

        public QueryIterator(K start, PageQuery<K, N, V> query, int pageSize)
        {
            this.start = start;
            this.query = query;
            this.pageSize = pageSize;

            fillNext();
        }

        @Override
        public boolean hasNext()
        {
            return next != null;
        }

        @Override
        public Row<K, N, V> next()
        {
            Row<K, N, V> result = next;
            if (result == null) {
                throw new NoSuchElementException();
            }

            fillNext();

            return result;
        }

        private void fillNext()
        {
            if (currentPage == null && lastPage) {
                next = null;
                return;
            }

            if (currentPage == null && !lastPage) {
                Iterable<? extends Row<K, N, V>> page = query.query(start, pageSize);
                currentPage = page.iterator();
                if (!firstPage && currentPage.hasNext()) {
                    // skip first entry, which was the last one from the previous page
                    currentPage.next();
                }
                firstPage = false;
            }

            if (currentPage != null) {
                next = Iterators.getNext(currentPage, null);

                if (next != null) {
                    count++;
                    start = next.getKey();
                }

                if (!currentPage.hasNext()) {
                    currentPage = null;
                    if (count < pageSize) {
                        lastPage = true;
                    }
                }
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
