package com.lmax.disruptor.dsl;

import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

import java.util.concurrent.Executor;

interface ConsumerInfo
{
    Sequence[] getSequences();

    SequenceBarrier getBarrier();

    boolean isEndOfChain();

    void start(Executor executor);

    void halt();

    /**
     * 将该EventProcessor设置为非链尾，将链尾标志设为false
     */
    void markAsUsedInBarrier();

    boolean isRunning();
}
