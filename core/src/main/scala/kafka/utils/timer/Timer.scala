/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.utils.timer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{DelayQueue, Executors, TimeUnit}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{KafkaThread, Time}

trait Timer {
  /**
    * 将给定的定时任务插入到时间轮上，等待后续延迟执行
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * @param timerTask the task to add
    */
  def add(timerTask: TimerTask): Unit

  /**
    * 向前推进时钟，执行已达过期时间的延迟任务
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * 获取时间轮上总的定时任务数
    * Get the number of tasks pending execution
    * @return the number of tasks
    */
  def size: Int

  /**
    * 关闭定时器
    * Shutdown the timer service, leaving pending tasks unexecuted
    */
  def shutdown(): Unit
}

/**
 * 是一个定时器类，封装了分层时间轮对象，为 Purgatory 提供延迟请求管理功能
 * 所谓的 Purgatory， 就是保存延迟请求的缓冲区，也就是说，它保存的是因为不满足条件而无法完成，但是又没有超时的请求
 * @param executorName Purgatory 的名字。 Kafka 中存在不同的 Purgatory,
 *                     <i>比如专门处理生产者延迟请求的 Produce 缓冲区、处理消费者延迟请求的 Fetch缓冲区等</i>
 *                     这里的 Produce 和 Fetch 就是 executorName
 * @param tickMs
 * @param wheelSize
 * @param startMs 该 SystemTimer 定时器启动时间，单位是毫秒。
 */
@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  /**
   * 单线程的线程池用于异步执行定时任务
   */
  // timeout timer
  private[this] val taskExecutor = Executors.newFixedThreadPool(1,
    (runnable: Runnable) => KafkaThread.nonDaemon("executor-" + executorName, runnable))

  /**
   * 延迟队列保存所有 Bucket, 即所有 TimerTaskList 对象
   */
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()

  /**
   * 总定时任务数
   */
  private[this] val taskCounter = new AtomicInteger(0)

  /**
   * 时间轮对象
   */
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  /**
   * 维护线程安全的读写锁
   */
  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  def add(timerTask: TimerTask): Unit = {
    // 获取读锁。在没有线程持有写锁的前提下，多个线程能够同时向时间轮添加定时任务
    readLock.lock()
    try {
      // 调用 addTimerTaskEntry 执行插入逻辑
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      // 释放读锁
      readLock.unlock()
    }
  }

  /**
   * 将给定的 {@param timerTaskEntry} 插入到时间轮中
   * 如果该 TimerTaskEntry 表征的定时任务没有过期或被取消，方法还会将已经过期的定时任务调教给线程池
   */
  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    // 是 timerTaskEntry 状态决定执行什么逻辑
    // 1. 未过期未取消: 添加到时间轮
    // 2. 已取消: 什么都不做
    // 3. 已过期: 提交到线程池，等待执行
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      // 定时任务未取消, 说明定时任务过期
      // 否则 timerWheel.add 方法应该返回 true
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    // 获取 delayQueue 中下一个已过期的 Bucket
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      // 获取写锁
      // 一旦线程持有写锁，其他任何线程执行 add/advanceClock 方法时会阻塞
      writeLock.lock()
      try {
        while (bucket != null) {
          // 推动时间轮向前 "滚动" 到 Bucket 的过期时间点
          timingWheel.advanceClock(bucket.getExpiration)
          // 将该Bucket 下的所有定时任务重写回到时间轮
          bucket.flush(addTimerTaskEntry)
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock() // 释放写锁
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown(): Unit = {
    taskExecutor.shutdown()
  }

}
