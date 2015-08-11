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
using System.Threading;
using System.Threading.Tasks;

namespace FallDave.EhSync
{
    /// <summary>
    /// A conceptual queue that enqueues and dequeues items asynchronously.
    /// </summary>
    /// <typeparam name="T">The type of value held by the queue.</typeparam>
    /// <remarks>
    /// <para>
    /// Dequeues return a <see cref="Task{T}"/>. If an item has already been enqueued, the task
    /// already contains a result. If no item has been enqueued yet, the task will complete once an
    /// item has been enqueued.
    /// </para>
    /// <para>
    /// The operations <see cref="EnqueueAsync"/> and <see cref="DequeueAsync"/> are usually
    /// lock-free, but either of these methods may need to trigger a grow operation to succeed, and
    /// the grow operation itself is not lock-free.
    /// </para>
    /// <para>
    /// The operations <see cref="TryEnqueue"/> and <see cref="TryDequeue"/> do not trigger grow
    /// operations and are always lock-free. These methods simply fail if a grow operation is needed
    /// to succeed, at which point <see cref="GrowAsync"/> or <see cref="EnsureCapacityAsync"/> can
    /// be used to explicitly trigger a grow operation.
    /// </para>
    /// <para>Implementation notes:</para>
    /// <para>
    /// This queue is implemented using a pair of internal queues that represent two sides of a
    /// "link", the input side (for enqueue) and the output side (for dequeue). An enqueue operation
    /// consumes the input side while a dequeue operation consumes the output side. When both sides
    /// of a link are consumed, the value from the enqueue operation is visible as the result of the
    /// task returned by the dequeue operation.
    /// </para>
    /// <para>
    /// Unused links must exist in the queues before it is possible to consume them. If the supply
    /// of unused input sides is exhausted upon an enqueue, or the supply of unused output sides is
    /// exhausted upon a dequeue, then a grow operation is necessary.
    /// </para>
    /// <para>
    /// A grow operation consists of creating unused links and adding their input and output sides
    /// to the respective queues. A grow operation must acquire a lock to ensure that links are
    /// added to both sides in the same order. A grow operation adds a number of links that is at
    /// least the specified growth increment.
    /// </para>
    /// <para>
    /// A grow operation can be explicitly triggered using <see cref="GrowAsync"/> or <see
    /// cref="EnsureCapacityAsync"/>. It may be desirable to run grow operations periodically from
    /// another thread.
    /// </para>
    /// </remarks>
    public class AsyncQueue<T>
    {
        private const int DefaultGrowthIncrement = 16;
        private readonly int growthIncrement;

        // The input and output sides of the links
        private readonly ConcurrentQueue<TaskCompletionSource<T>> inputs, outputs;

        // Synchronizes adds to inputs and outputs queues
        private readonly MuxProducer<TaskCompletionSource<T>> producer;

        // The counts of ready links are tracked separately to avoid the O(n) counting of the queues.
        private long inputsReady = 0;

        private long outputsReady = 0;

        #region main

        /// <summary>
        /// Creates an <see cref="AsyncQueue{T}"/> with the specified growth increment.
        /// </summary>
        /// <param name="growthIncrement">The number of elements between reallocations.</param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="growthIncrement"/> was less than 1.
        /// </exception>
        public AsyncQueue(int growthIncrement)
        {
            this.inputs = new ConcurrentQueue<TaskCompletionSource<T>>();
            this.outputs = new ConcurrentQueue<TaskCompletionSource<T>>();
            this.producer = new MuxProducer<TaskCompletionSource<T>>(inputs, outputs);

            if (growthIncrement < 1)
            {
                throw new ArgumentOutOfRangeException("growIncrement");
            }

            this.growthIncrement = growthIncrement;
            GrowTo(growthIncrement).Wait();
        }

        /// <summary>
        /// Creates an <see cref="AsyncQueue{T}"/> with a default growth increment.
        /// </summary>
        public AsyncQueue()
            : this(DefaultGrowthIncrement)
        {
        }

        /// <summary>
        /// Removes an item from this queue asynchronously.
        /// </summary>
        /// <returns>A <see cref="Task{T}"/> whose result is an item removed from this queue.</returns>
        public async Task<T> DequeueAsync()
        {
            var link = await EnsureGetUncounted(outputs);
            return await link.Task;
        }

        /// <summary>
        /// Adds a new item to this queue asynchronously.
        /// </summary>
        /// <param name="item">
        /// An item to be added to this queue. <see langword="null"/> values are allowed.
        /// </param>
        /// <returns>A task to await the item being added to this queue.</returns>
        public async Task EnqueueAsync(T item)
        {
            var link = await EnsureGetUncounted(inputs);
            link.SetResult(item);
        }

        /// <summary>
        /// Attempts to prepare this queue so that at least specified number of enqueues and of
        /// dequeues will not require a grow operation to succeed.
        /// </summary>
        /// <param name="count">
        /// The minimum number of ready enqueues and of ready dequeues to allocate before finishing.
        /// </param>
        /// <returns>A task to await the end of the grow operation.</returns>
        public async Task EnsureCapacityAsync(int count)
        {
            if (Interlocked.Read(ref inputsReady) < count || Interlocked.Read(ref outputsReady) < count)
            {
                await GrowTo(count);
            }
        }

        /// <summary>
        /// Attempts to prepare this queue so that the default number (the growth increment) of
        /// enqueues and of dequeues will not require a grow operation to succeed.
        /// </summary>
        /// <returns>A task to await the end of the grow operation.</returns>
        public async Task GrowAsync()
        {
            await EnsureCapacityAsync(growthIncrement);
        }

        /// <summary>
        /// Attempts to produce a task which removes an item from this queue asynchronously.
        /// </summary>
        /// <param name="task">
        /// A <see cref="Task{T}"/> whose result is an item removed from this queue.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the dequeue succeeded; <see langword="false"/> if there were
        /// no available links (a grow operation may be necessary).
        /// </returns>
        /// <remarks>
        /// This method is guaranteed lock-free, but will fail if a grow operation would be needed
        /// to succeed.
        /// 
        /// This method is not asynchronous, but the produced task itself completes asynchronously.
        /// </remarks>
        public bool TryDequeue(out Task<T> task)
        {
            TaskCompletionSource<T> link;
            if (TryGetOutput(out link))
            {
                task = link.Task;
                return true;
            }
            task = null;
            return false;
        }

        /// <summary>
        /// Attempts to add a new item to this queue.
        /// </summary>
        /// <param name="item">
        /// An item to be added to this queue. <see langword="null"/> values are allowed.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the item was enqueued successfully; <see langword="false"/> if
        /// there were no available links (a grow operation may be necessary).
        /// </returns>
        /// <remarks>
        /// This method is guaranteed lock-free, but will fail if a grow operation would be needed
        /// to succeed.
        /// </remarks>
        public bool TryEnqueue(T item)
        {
            TaskCompletionSource<T> link;
            if (TryGetInput(out link))
            {
                link.SetResult(item);
                return true;
            }
            return false;
        }

        #endregion main

        #region interior growth implementation

        // Add unused links to the queue until at least count unused links are available on inputs
        // and outputs. This is "Grow To" and not "Grow By" to prevent several tasks from lining up
        // and each adding the entire count.
        private async Task GrowTo(int count)
        {
            if (count >= 0)
            {
                await producer.AddRangeAsync(GenerateLinksToAdd(count));
            }
        }

        // Repeatedly yields unused links until the target count is reached.
        private IEnumerable<TaskCompletionSource<T>> GenerateLinksToAdd(int targetCount)
        {
            long lastInputsReady = Interlocked.Read(ref inputsReady);
            long lastOutputsReady = Interlocked.Read(ref outputsReady);

            while (lastInputsReady < targetCount || lastOutputsReady < targetCount)
            {
                yield return new TaskCompletionSource<T>();
                lastInputsReady = Interlocked.Increment(ref inputsReady);
                lastOutputsReady = Interlocked.Increment(ref outputsReady);
            }
        }

        #endregion interior growth implementation

        #region interior auto-grow en/de

        // Dequeues an element from queue, calling GrowTo(growAmount) to add unused links if
        // needed. Does not update ready counts.
        private async Task<TaskCompletionSource<T>> EnsureGetUncounted(ConcurrentQueue<TaskCompletionSource<T>> queue)
        {
            TaskCompletionSource<T> link;

            while (!queue.TryDequeue(out link))
            {
                await GrowTo(growthIncrement);
            }

            return link;
        }

        // Gets an item from inputs, growing if necessary, decrementing the input ready count first.
        private async Task<TaskCompletionSource<T>> GetInputAsync()
        {
            Interlocked.Decrement(ref inputsReady);
            return await EnsureGetUncounted(inputs);
        }

        // Gets an item from outputs, growing if necessary, decrementing the output ready count first.
        private async Task<TaskCompletionSource<T>> GetOutputAsync()
        {
            Interlocked.Decrement(ref outputsReady);
            return await EnsureGetUncounted(outputs);
        }

        #endregion interior auto-grow en/de

        #region interior lock-free en/de

        // Try to remove an item and decrement a count.
        // If no item is available, leave the count as it was and fail. Do not grow.
        private bool TryGetCounted(ConcurrentQueue<TaskCompletionSource<T>> queue, ref long readyCount, out TaskCompletionSource<T> link)
        {
            Interlocked.Decrement(ref readyCount);
            if (!queue.TryDequeue(out link))
            {
                Interlocked.Increment(ref readyCount);
                return false;
            }
            return true;
        }

        // Gets an item from inputs, decrementing the input ready count first. On failure, restores the ready count.
        private bool TryGetInput(out TaskCompletionSource<T> link)
        {
            return TryGetCounted(inputs, ref inputsReady, out link);
        }

        // Gets an item from outputs, decrementing the output ready count first. On failure, restores the ready count.
        private bool TryGetOutput(out TaskCompletionSource<T> link)
        {
            return TryGetCounted(outputs, ref outputsReady, out link);
        }

        #endregion interior lock-free en/de
    }
}