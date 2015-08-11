/*
 * FallDave.EhSync - Dubious async tools for C#
 * 
 * Written in 2015 by David McFall <dvmcfall@gmail.com>
 * 
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to the
 * public domain worldwide. This software is distributed without any
 * warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication
 * along with this software. If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */


using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FallDave.EhSync
{
    internal class MuxProducer<T>
    {
        private readonly IProducerConsumerCollection<T>[] collections;
        private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1);

        private static IProducerConsumerCollection<T>[] CloneArray(IProducerConsumerCollection<T>[] src)
        {
            return (IProducerConsumerCollection<T>[])src.Clone();
        }

        public MuxProducer(params IProducerConsumerCollection<T>[] collections)
        {
            if (collections == null)
            {
                throw new ArgumentNullException("collections");
            }

            int index = -1;
            foreach (var collection in collections)
            {
                ++index;
                if (collection == null)
                {
                    throw new ArgumentNullException(String.Format("collections[{0}]", index));
                }
            }

            this.collections = CloneArray(collections);
        }

        private void UnlockedAdd(T item)
        {
            var failedIndices = collections
                .Select((collection, index) => new { index, succeeded = collection.TryAdd(item) })
                .Where(r => !r.succeeded)
                .Select(r => r.index)
                .ToArray();


            if (failedIndices.Length > 0)
            {
                string failedIndicesStr = String.Join(", ", failedIndices);
                string message = String.Format("The following {0} of {1} consumer(s) failed to accept added element: {2}", failedIndices.Length, collections.Length, failedIndicesStr);

                throw new InvalidOperationException(message);
            }
        }

        private int UnlockedAddRange(IEnumerable<T> items)
        {
            int added = 0;

            foreach (var item in items)
            {
                UnlockedAdd(item);
                ++added;
            }

            return added;
        }

        private static void ItemsNotNull(object items)
        {
            if (items == null)
            {
                throw new ArgumentNullException("items");
            }
        }

        private int AddRangeThenRelease(IEnumerable<T> items)
        {
            try
            {
                return UnlockedAddRange(items);
            }
            finally
            {
                semaphore.Release();
            }
        }

        public int AddRange(IEnumerable<T> items)
        {
            ItemsNotNull(items);
            semaphore.Wait();
            return AddRangeThenRelease(items);
        }

        public async Task<int> AddRangeAsync(IEnumerable<T> items)
        {
            ItemsNotNull(items);
            await semaphore.WaitAsync();
            return AddRangeThenRelease(items);
        }

        public int Add(params T[] items)
        {
            ItemsNotNull(items);
            if (items.Length == 0)
            {
                return 0;
            }
            semaphore.Wait();
            return AddRangeThenRelease(items);
        }

        public async Task<int> AddAsync(params T[] items)
        {
            ItemsNotNull(items);
            if (items.Length == 0)
            {
                return 0;
            }
            await semaphore.WaitAsync();
            return AddRangeThenRelease(items);
        }

    }
}
