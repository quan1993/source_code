/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor;

import java.util.concurrent.locks.LockSupport;

import sun.misc.Unsafe;

import com.lmax.disruptor.util.Util;


/**
 * <p>Coordinator for claiming sequences for access to a data structure while tracking dependent {@link Sequence}s.
 * Suitable for use for sequencing across multiple publisher threads.</p>
 *
 * <p> * Note on {@link Sequencer#getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@link Sequencer#next()}, to determine the highest available sequence that can be read, then
 * {@link Sequencer#getHighestPublishedSequence(long, long)} should be used.</p>
 */
public final class MultiProducerSequencer extends AbstractSequencer
{
	private static final Unsafe UNSAFE = Util.getUnsafe();

	/***
	 * Returns the offset of the first element for a given array class.
	 * To access elements of the array class, this value may be used along with
	 * with that returned by 
	 * <a href="#arrayIndexScale"><code>arrayIndexScale</code></a>,
	 * if non-zero.<br>
	 * 获取给定数组中第一个元素的偏移地址。
	 * 为了存取数组中的元素，这个偏移地址与<a href="#arrayIndexScale"><code>arrayIndexScale
	 * </code></a>方法的非0返回值一起被使用。
	 * @param arrayClass the class for which the first element's address should
	 *                   be obtained.
	 *                   第一个元素地址被获取的class
	 * @return the offset of the first element of the array class.
	 *    数组第一个元素 的偏移地址
	 * @see arrayIndexScale(Class)
        public native int arrayBaseOffset(Class arrayClass);
	 */
	private static final long BASE = UNSAFE.arrayBaseOffset(int[].class);

	/***
	 * Returns the scale factor used for addressing elements of the supplied
	 * array class.  Where a suitable scale factor can not be returned (e.g.
	 * for primitive types), zero should be returned.  The returned value
	 * can be used with 
	 * <a href="#arrayBaseOffset"><code>arrayBaseOffset</code></a>
	 * to access elements of the class.<br>
	 * 获取用户给定数组寻址的换算因子.一个合适的换算因子不能返回的时候(例如：基本类型),
	 * 返回0.这个返回值能够与<a href="#arrayBaseOffset"><code>arrayBaseOffset</code>
	 * </a>一起使用去存取这个数组class中的元素
	 * 
	 * @param arrayClass the class whose scale factor should be returned.
	 * @return the scale factor, or zero if not supported for this array class.
	 * 
	 * public native int arrayIndexScale(Class arrayClass);
	 * 
	 */
	private static final long SCALE = UNSAFE.arrayIndexScale(int[].class);

	private final Sequence gatingSequenceCache = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

	/**
	 *  availableBuffer : tracks(追踪) the state of each ringbuffer slot(位置)
	 *  
	 */
	// see below for more details on the approach
	private final int[] availableBuffer;
	/**
	 * bufferSize - 1, 即RingBuffer下标的最大值
	 * 
	 */
	private final int indexMask;
	/**
	 * log(2, bufferSize), 
	 * 
	 */
	private final int indexShift;

	/**
	 * Construct a Sequencer with the selected wait strategy and buffer size.
	 *
	 * @param bufferSize   the size of the buffer that this will sequence over.
	 * @param waitStrategy for those waiting on sequences.
	 */
	public MultiProducerSequencer(int bufferSize, final WaitStrategy waitStrategy)
	{
		super(bufferSize, waitStrategy);
		availableBuffer = new int[bufferSize];
		indexMask = bufferSize - 1;
		indexShift = Util.log2(bufferSize);
		initialiseAvailableBuffer();
	}

	/**
	 * @see Sequencer#hasAvailableCapacity(int)
	 */
	@Override
	public boolean hasAvailableCapacity(final int requiredCapacity)
	{
		return hasAvailableCapacity(gatingSequences, requiredCapacity, cursor.get());
	}

	private boolean hasAvailableCapacity(Sequence[] gatingSequences, final int requiredCapacity, long cursorValue)
	{
		long wrapPoint = (cursorValue + requiredCapacity) - bufferSize;
		long cachedGatingSequence = gatingSequenceCache.get();

		if (wrapPoint > cachedGatingSequence || cachedGatingSequence > cursorValue)
		{
			long minSequence = Util.getMinimumSequence(gatingSequences, cursorValue);
			gatingSequenceCache.set(minSequence);

			if (wrapPoint > minSequence)
			{
				return false;
			}
		}

		return true;
	}

	/**
	 * @see Sequencer#claim(long)
	 */
	@Override
	public void claim(long sequence)
	{
		cursor.set(sequence);
	}

	/**
	 * @see Sequencer#next()
	 */
	@Override
	public long next()
	{
		return next(1);
	}

	/**
	 * @see Sequencer#next(int)
	 */
	@Override
	public long next(int n)
	{
		if (n < 1)
		{
			throw new IllegalArgumentException("n must be > 0");
		}

		long current;
		long next;

		do
		{
			/**
			 * 当前游标
			 */
			current = cursor.get();
			/**
			 * 要发布的游标
			 */
			next = current + n;
			/**
			 * 覆盖点
			 */
			long wrapPoint = next - bufferSize;
			/**
			 * 消费者中最小的sequence，   保存的是刚消费过的
			 */
			long cachedGatingSequence = gatingSequenceCache.get();
			/**
			 * 缓存已满  或者 处理器处理异常<br>
			 * 
			 * 当wrapPoint < 0时，表示游标未超过RingBuffer结尾，
			 * 因为cachedGatingSequence代表消费者中最小的sequence，肯定>0
			 * 所以next的值肯定比cachedGatingSequence大，且缓存必然未满
			 * （程序正常运行，生产者的游标必然时大于等下消费者的游标的，而next=current+n，
			 * 所以next必然大于消费者游标大于cachedGatingSequence）
			 * 
			 * 当wrapPoint > 0时，表示游标已超过RingBuffer结尾，此时只要覆盖点小于等于消费者中最小的sequence即可，<br>
			 * 即wrap <= cachedGatingSequence 
			 * 因此，缓存已满的条件是wrapPoint > cachedGatingSequence
			 * 
			 */
			if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current)
			{
				long gatingSequence = Util.getMinimumSequence(gatingSequences, current);

				/**
				 * 缓存区满，继续再次尝试
				 */
				if (wrapPoint > gatingSequence)
				{
					waitStrategy.signalAllWhenBlocking();
					LockSupport.parkNanos(1); // TODO, should we spin based on the wait strategy?
					continue;
				}

				/**
				 * 更新当前生产者发布的最大序列
				 */
				gatingSequenceCache.set(gatingSequence);
			}
			else if (cursor.compareAndSet(current, next))
			{
				/**
				 * 成功获取到发布序列并设置当前游标成功时跳出循环
				 */
				break;
			}
		}
		while (true);

		return next;
	}

	/**
	 * @see Sequencer#tryNext()
	 */
	@Override
	public long tryNext() throws InsufficientCapacityException
	{
		return tryNext(1);
	}

	/**
	 * @see Sequencer#tryNext(int)
	 */
	@Override
	public long tryNext(int n) throws InsufficientCapacityException
	{
		if (n < 1)
		{
			throw new IllegalArgumentException("n must be > 0");
		}

		long current;
		long next;

		do
		{
			current = cursor.get();
			next = current + n;

			if (!hasAvailableCapacity(gatingSequences, n, current))
			{
				throw InsufficientCapacityException.INSTANCE;
			}
		}
		while (!cursor.compareAndSet(current, next));

		return next;
	}

	/**
	 * @see Sequencer#remainingCapacity()
	 */
	@Override
	public long remainingCapacity()
	{
		long consumed = Util.getMinimumSequence(gatingSequences, cursor.get());
		long produced = cursor.get();
		return getBufferSize() - (produced - consumed);
	}

	private void initialiseAvailableBuffer()
	{
		for (int i = availableBuffer.length - 1; i != 0; i--)
		{
			setAvailableBufferValue(i, -1);
		}

		setAvailableBufferValue(0, -1);
	}

	/**
	 * @see Sequencer#publish(long)
	 */
	@Override
	public void publish(final long sequence)
	{
		setAvailable(sequence);
		waitStrategy.signalAllWhenBlocking();
	}

	/**
	 * @see Sequencer#publish(long, long)
	 */
	@Override
	public void publish(long lo, long hi)
	{
		for (long l = lo; l <= hi; l++)
		{
			setAvailable(l);
		}
		waitStrategy.signalAllWhenBlocking();
	}

	/**
	 * The below methods work on the availableBuffer flag.
	 * <p>
	 * The prime reason is to avoid a shared sequence object between publisher threads.
	 * (Keeping single pointers tracking start and end would require coordination
	 * between the threads).
	 * <p>
	 * --  Firstly we have the constraint that the delta between the cursor and minimum
	 * gating sequence will never be larger than the buffer size (the code in
	 * next/tryNext in the Sequence takes care of that).
	 * -- Given that; take the sequence value and mask off the lower portion of the
	 * sequence as the index into the buffer (indexMask). (aka modulo operator)
	 * -- The upper portion of the sequence becomes the value to check for availability.
	 * ie: it tells us how many times around the ring buffer we've been (aka division)
	 * -- Because we can't wrap without the gating sequences moving forward (i.e. the
	 * minimum gating sequence is effectively our last available position in the
	 * buffer), when we have new data and successfully claimed a slot we can simply
	 * write over the top.
	 */
	private void setAvailable(final long sequence)
	{
		setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
	}

	private void setAvailableBufferValue(int index, int flag)
	{
		long bufferAddress = (index * SCALE) + BASE;
		/***
		 * Sets the value of the integer field at the specified offset in the
		 * supplied object to the given value.  This is an ordered or lazy
		 * version of <code>putIntVolatile(Object,long,int)</code>, which
		 * doesn't guarantee the immediate visibility of the change to other
		 * threads.  It is only really useful where the integer field is
		 * <code>volatile</code>, and is thus expected to change unexpectedly.
		 * 设置obj对象中offset偏移地址对应的整型field的值为指定值。这是一个有序或者
		 * 有延迟的<code>putIntVolatile</cdoe>方法，并且不保证值的改变被其他线程立
		 * 即看到。只有在field被<code>volatile</code>修饰并且期望被意外修改的时候
		 * 使用才有用。
		 * 
		 * @param obj the object containing the field to modify.
		 *    包含需要修改field的对象
		 * @param offset the offset of the integer field within <code>obj</code>.
		 *       <code>obj</code>中整型field的偏移量
		 * @param value the new value of the field.
		 *      field将被设置的新值
		 * @see #putIntVolatile(Object,long,int)
		  public native void putOrderedInt(Object obj, long offset, int value);
		 */
		UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag);
	}

	/**
	 * @see Sequencer#isAvailable(long)
	 */
	@Override
	public boolean isAvailable(long sequence)
	{
		int index = calculateIndex(sequence);
		int flag = calculateAvailabilityFlag(sequence);
		/**
		 * 获得RingBuffer中该元素在磁盘中的位置
		 */
		long bufferAddress = (index * SCALE) + BASE;
		return UNSAFE.getIntVolatile(availableBuffer, bufferAddress) == flag;
	}

	@Override
	public long getHighestPublishedSequence(long lowerBound, long availableSequence)
	{
		for (long sequence = lowerBound; sequence <= availableSequence; sequence++)
		{
			if (!isAvailable(sequence))
			{
				return sequence - 1;
			}
		}

		return availableSequence;
	}

	/**
	 * 可用性
	 * calculateAvailabilityFlag是获取当前是圈了. 
                    因为indexShift = Util.log2(bufferSize); 
                   这里的bufferSize和RingBuffer的bufferSize是一样的 
        >>> indexShift和log2操作是对应的.sequence右移indexShift位,其实即时除以了数组的大小,意思就是第几圈了. 
	 */
	private int calculateAvailabilityFlag(final long sequence)
	{
		return (int) (sequence >>> indexShift);
	}

	/**
	 * 与RingBuffer下标最大值相与，确保sequence值符合范围
	 * @param sequence
	 * @return
	 */
	private int calculateIndex(final long sequence)
	{
		return ((int) sequence) & indexMask;
	}
}
