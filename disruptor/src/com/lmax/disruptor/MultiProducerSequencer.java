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
	 * ��ȡ���������е�һ��Ԫ�ص�ƫ�Ƶ�ַ��
	 * Ϊ�˴�ȡ�����е�Ԫ�أ����ƫ�Ƶ�ַ��<a href="#arrayIndexScale"><code>arrayIndexScale
	 * </code></a>�����ķ�0����ֵһ��ʹ�á�
	 * @param arrayClass the class for which the first element's address should
	 *                   be obtained.
	 *                   ��һ��Ԫ�ص�ַ����ȡ��class
	 * @return the offset of the first element of the array class.
	 *    �����һ��Ԫ�� ��ƫ�Ƶ�ַ
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
	 * ��ȡ�û���������Ѱַ�Ļ�������.һ�����ʵĻ������Ӳ��ܷ��ص�ʱ��(���磺��������),
	 * ����0.�������ֵ�ܹ���<a href="#arrayBaseOffset"><code>arrayBaseOffset</code>
	 * </a>һ��ʹ��ȥ��ȡ�������class�е�Ԫ��
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
	 *  availableBuffer : tracks(׷��) the state of each ringbuffer slot(λ��)
	 *  
	 */
	// see below for more details on the approach
	private final int[] availableBuffer;
	/**
	 * bufferSize - 1, ��RingBuffer�±�����ֵ
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
			 * ��ǰ�α�
			 */
			current = cursor.get();
			/**
			 * Ҫ�������α�
			 */
			next = current + n;
			/**
			 * ���ǵ�
			 */
			long wrapPoint = next - bufferSize;
			/**
			 * ����������С��sequence��   ������Ǹ����ѹ���
			 */
			long cachedGatingSequence = gatingSequenceCache.get();
			/**
			 * ��������  ���� �����������쳣<br>
			 * 
			 * ��wrapPoint < 0ʱ����ʾ�α�δ����RingBuffer��β��
			 * ��ΪcachedGatingSequence��������������С��sequence���϶�>0
			 * ����next��ֵ�϶���cachedGatingSequence���һ����Ȼδ��
			 * �������������У������ߵ��α��Ȼʱ���ڵ��������ߵ��α�ģ���next=current+n��
			 * ����next��Ȼ�����������α����cachedGatingSequence��
			 * 
			 * ��wrapPoint > 0ʱ����ʾ�α��ѳ���RingBuffer��β����ʱֻҪ���ǵ�С�ڵ�������������С��sequence���ɣ�<br>
			 * ��wrap <= cachedGatingSequence 
			 * ��ˣ�����������������wrapPoint > cachedGatingSequence
			 * 
			 */
			if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current)
			{
				long gatingSequence = Util.getMinimumSequence(gatingSequences, current);

				/**
				 * ���������������ٴγ���
				 */
				if (wrapPoint > gatingSequence)
				{
					waitStrategy.signalAllWhenBlocking();
					LockSupport.parkNanos(1); // TODO, should we spin based on the wait strategy?
					continue;
				}

				/**
				 * ���µ�ǰ�����߷������������
				 */
				gatingSequenceCache.set(gatingSequence);
			}
			else if (cursor.compareAndSet(current, next))
			{
				/**
				 * �ɹ���ȡ���������в����õ�ǰ�α�ɹ�ʱ����ѭ��
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
		 * ����obj������offsetƫ�Ƶ�ַ��Ӧ������field��ֵΪָ��ֵ������һ���������
		 * ���ӳٵ�<code>putIntVolatile</cdoe>���������Ҳ���ֵ֤�ĸı䱻�����߳���
		 * ��������ֻ����field��<code>volatile</code>���β��������������޸ĵ�ʱ��
		 * ʹ�ò����á�
		 * 
		 * @param obj the object containing the field to modify.
		 *    ������Ҫ�޸�field�Ķ���
		 * @param offset the offset of the integer field within <code>obj</code>.
		 *       <code>obj</code>������field��ƫ����
		 * @param value the new value of the field.
		 *      field�������õ���ֵ
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
		 * ���RingBuffer�и�Ԫ���ڴ����е�λ��
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
	 * ������
	 * calculateAvailabilityFlag�ǻ�ȡ��ǰ��Ȧ��. 
                    ��ΪindexShift = Util.log2(bufferSize); 
                   �����bufferSize��RingBuffer��bufferSize��һ���� 
        >>> indexShift��log2�����Ƕ�Ӧ��.sequence����indexShiftλ,��ʵ��ʱ����������Ĵ�С,��˼���ǵڼ�Ȧ��. 
	 */
	private int calculateAvailabilityFlag(final long sequence)
	{
		return (int) (sequence >>> indexShift);
	}

	/**
	 * ��RingBuffer�±����ֵ���룬ȷ��sequenceֵ���Ϸ�Χ
	 * @param sequence
	 * @return
	 */
	private int calculateIndex(final long sequence)
	{
		return ((int) sequence) & indexMask;
	}
}
