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

package kafka.network

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent._

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.network
import kafka.server.KafkaConfig
import kafka.utils.{Logging, NotNothing, Pool}
import kafka.utils.Implicits._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData._
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors, ObjectSerializationCache}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Sanitizer, Time}

import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object RequestChannel extends Logging {
  private val requestLogger = Logger("kafka.request.logger")

  val RequestQueueSizeMetric = "RequestQueueSize"
  val ResponseQueueSizeMetric = "ResponseQueueSize"
  val ProcessorMetricTag = "processor"

  def isRequestLoggingEnabled: Boolean = requestLogger.underlying.isDebugEnabled

  sealed trait BaseRequest // 类似Java的interface BaseRequest是一个trait接口， 有两个实现类 ShutdownRequest和 Request
  /**
   * ShutdownRequest类仅仅起到一个标志位的作用
   * 当Broker进程关闭时，请求处理器会发送ShutdownRequest到专属的请求处理线程。
   * 该线程接收到此请求后，会出发一系列的Broker关闭逻辑
   */
  case object ShutdownRequest extends BaseRequest

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress) {
    val sanitizedUser: String = Sanitizer.sanitize(principal.getName)
  }

  class Metrics(enabledApis: Iterable[ApiKeys]) {
    def this(scope: ListenerType) = {
      this(ApiKeys.apisForListener(scope).asScala)
    }

    private val metricsMap = mutable.Map[String, RequestMetrics]()

    (enabledApis.map(_.name) ++
      Seq(RequestMetrics.consumerFetchMetricName, RequestMetrics.followFetchMetricName)).foreach { name =>
      metricsMap.put(name, new RequestMetrics(name))
    }

    def apply(metricName: String): RequestMetrics = metricsMap(metricName)

    def close(): Unit = {
       metricsMap.values.foreach(_.removeMetrics())
    }
  }

  /**
   * Request 是真正定义各类Clients或Broker端请求的实现类。
   * @param processor 这个请求是由哪个Processor线程接收处理的， Broker端参数
   *                  num.network.threads 控制了Broker每个监听器上创建的Processor线程数，编号分别为0、1、2、...
   *                  这里就指定了处理请求的线程序号，并且当Request被后面的I/O线程处理完毕后，还要依靠Processor线程发送
   *                  Response给请求发送方，因此,Request中必须记录它之前是被哪个Processor线程接收的。
   *                  Processor线程仅仅是网络接收线程，不会执行真正的Request请求逻辑, 那是I/O线程负责的事情
   * @param context  标识请求上下文信息的, 它保存了有关Request的所有上下文信息 {@link RequestContext}
   * @param startTimeNanos 记录了Request对象被创建的时间，主要用户各种时间统计指标的计算
   *                       请求对象中的很多JMX指标，特别是时间类的统计指标，都需要通过
   *                       startTimeNanos字段。以纳秒为单位的时间戳信息，可以实现非常细粒度的时间统计经度
   * @param memoryPool 一个非阻塞式的内存缓冲区，主要作用是避免Request对象无限使用内存
   *                   该内存缓存区的接口类和实现类，分别是{@link MemoryPool} 和 {@link org.apache.kafka.common.memory.SimpleMemoryPool}
   *                   {@link org.apache.kafka.common.memory.SimpleMemoryPool#tryAllocate}
   * @param buffer 真正保存Request对象内容的字节缓冲区。Request发送方必须按照Kafka RPC 协议规定的格式向该缓冲区写入字节，
   *               否则将抛出{@link org.apache.kafka.common.errors.InvalidRequestException}异常。
   *               这个逻辑主要是 {@link RequestContext#parseRequest} 方法实现的
   * @param metrics Request相关的各种监控指标的一个管理类。内部构建了一个Map，封装了所有的请求JMX指标。
   * @param envelope
   */
  class Request(val processor: Int,
                val context: RequestContext,
                val startTimeNanos: Long,
                val memoryPool: MemoryPool,
                @volatile var buffer: ByteBuffer,
                metrics: RequestChannel.Metrics,
                val envelope: Option[RequestChannel.Request] = None) extends BaseRequest {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    @volatile var requestDequeueTimeNanos = -1L
    @volatile var apiLocalCompleteTimeNanos = -1L
    @volatile var responseCompleteTimeNanos = -1L
    @volatile var responseDequeueTimeNanos = -1L
    @volatile var messageConversionsTimeNanos = 0L
    @volatile var apiThrottleTimeMs = 0L
    @volatile var temporaryMemoryBytes = 0L
    @volatile var recordNetworkThreadTimeCallback: Option[Long => Unit] = None

    val session = Session(context.principal, context.clientAddress)

    private val bodyAndSize: RequestAndSize = context.parseRequest(buffer)

    // This is constructed on creation of a Request so that the JSON representation is computed before the request is
    // processed by the api layer. Otherwise, a ProduceRequest can occur without its data (ie. it goes into purgatory).
    val requestLog: Option[JsonNode] =
      if (RequestChannel.isRequestLoggingEnabled) Some(RequestConvertToJson.request(loggableRequest))
      else None

    def header: RequestHeader = context.header

    def sizeOfBodyInBytes: Int = bodyAndSize.size

    def sizeInBytes: Int = header.size(new ObjectSerializationCache) + sizeOfBodyInBytes

    //most request types are parsed entirely into objects at this point. for those we can release the underlying buffer.
    //some (like produce, or any time the schema contains fields of types BYTES or NULLABLE_BYTES) retain a reference
    //to the buffer. for those requests we cannot release the buffer early, but only when request processing is done.
    if (!header.apiKey.requiresDelayedAllocation) {
      releaseBuffer()
    }

    def isForwarded: Boolean = envelope.isDefined

    def buildResponseSend(abstractResponse: AbstractResponse): Send = {
      envelope match {
        case Some(request) =>
          val responseBytes = context.buildResponseEnvelopePayload(abstractResponse)
          val envelopeResponse = new EnvelopeResponse(responseBytes, Errors.NONE)
          request.context.buildResponseSend(envelopeResponse)
        case None =>
          context.buildResponseSend(abstractResponse)
      }
    }

    def responseNode(response: AbstractResponse): Option[JsonNode] = {
      if (RequestChannel.isRequestLoggingEnabled)
        Some(RequestConvertToJson.response(response, context.apiVersion))
      else
        None
    }

    def headerForLoggingOrThrottling(): RequestHeader = {
      envelope match {
        case Some(request) =>
          request.context.header
        case None =>
          context.header
      }
    }

    def requestDesc(details: Boolean): String = {
      val forwardDescription = envelope.map { request =>
        s"Forwarded request: ${request.context} "
      }.getOrElse("")
      s"$forwardDescription$header -- ${loggableRequest.toString(details)}"
    }

    def body[T <: AbstractRequest](implicit classTag: ClassTag[T], @nowarn("cat=unused") nn: NotNothing[T]): T = {
      bodyAndSize.request match {
        case r: T => r
        case r =>
          throw new ClassCastException(s"Expected request with type ${classTag.runtimeClass}, but found ${r.getClass}")
      }
    }

    def loggableRequest: AbstractRequest = {

      bodyAndSize.request match {
        case alterConfigs: AlterConfigsRequest =>
          val loggableConfigs = alterConfigs.configs().asScala.map { case (resource, config) =>
            val loggableEntries = new AlterConfigsRequest.Config(config.entries.asScala.map { entry =>
                new AlterConfigsRequest.ConfigEntry(entry.name, KafkaConfig.loggableValue(resource.`type`, entry.name, entry.value))
            }.asJavaCollection)
            (resource, loggableEntries)
          }.asJava
          new AlterConfigsRequest.Builder(loggableConfigs, alterConfigs.validateOnly).build(alterConfigs.version())

        case alterConfigs: IncrementalAlterConfigsRequest =>
          val resources = new AlterConfigsResourceCollection(alterConfigs.data.resources.size)
          alterConfigs.data.resources.forEach { resource =>
            val newResource = new AlterConfigsResource()
              .setResourceName(resource.resourceName)
              .setResourceType(resource.resourceType)
            resource.configs.forEach { config =>
              newResource.configs.add(new AlterableConfig()
                .setName(config.name)
                .setValue(KafkaConfig.loggableValue(ConfigResource.Type.forId(resource.resourceType), config.name, config.value))
                .setConfigOperation(config.configOperation))
            }
            resources.add(newResource)
          }
          val data = new IncrementalAlterConfigsRequestData()
            .setValidateOnly(alterConfigs.data.validateOnly())
            .setResources(resources)
          new IncrementalAlterConfigsRequest.Builder(data).build(alterConfigs.version)

        case _ =>
          bodyAndSize.request
      }
    }

    trace(s"Processor $processor received request: ${requestDesc(true)}")

    def requestThreadTimeNanos: Long = {
      if (apiLocalCompleteTimeNanos == -1L) apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds
      math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L)
    }

    def updateRequestMetrics(networkThreadTimeNanos: Long, response: Response): Unit = {
      val endTimeNanos = Time.SYSTEM.nanoseconds

      /**
       * Converts nanos to millis with micros precision as additional decimal places in the request log have low
       * signal to noise ratio. When it comes to metrics, there is little difference either way as we round the value
       * to the nearest long.
       */
      def nanosToMs(nanos: Long): Double = {
        val positiveNanos = math.max(nanos, 0)
        TimeUnit.NANOSECONDS.toMicros(positiveNanos).toDouble / TimeUnit.MILLISECONDS.toMicros(1)
      }

      val requestQueueTimeMs = nanosToMs(requestDequeueTimeNanos - startTimeNanos)
      val apiLocalTimeMs = nanosToMs(apiLocalCompleteTimeNanos - requestDequeueTimeNanos)
      val apiRemoteTimeMs = nanosToMs(responseCompleteTimeNanos - apiLocalCompleteTimeNanos)
      val responseQueueTimeMs = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos)
      val responseSendTimeMs = nanosToMs(endTimeNanos - responseDequeueTimeNanos)
      val messageConversionsTimeMs = nanosToMs(messageConversionsTimeNanos)
      val totalTimeMs = nanosToMs(endTimeNanos - startTimeNanos)
      val fetchMetricNames =
        if (header.apiKey == ApiKeys.FETCH) {
          val isFromFollower = body[FetchRequest].isFromFollower
          Seq(
            if (isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          )
        }
        else Seq.empty
      val metricNames = fetchMetricNames :+ header.apiKey.name
      metricNames.foreach { metricName =>
        val m = metrics(metricName)
        m.requestRate(header.apiVersion).mark()
        m.requestQueueTimeHist.update(Math.round(requestQueueTimeMs))
        m.localTimeHist.update(Math.round(apiLocalTimeMs))
        m.remoteTimeHist.update(Math.round(apiRemoteTimeMs))
        m.throttleTimeHist.update(apiThrottleTimeMs)
        m.responseQueueTimeHist.update(Math.round(responseQueueTimeMs))
        m.responseSendTimeHist.update(Math.round(responseSendTimeMs))
        m.totalTimeHist.update(Math.round(totalTimeMs))
        m.requestBytesHist.update(sizeOfBodyInBytes)
        m.messageConversionsTimeHist.foreach(_.update(Math.round(messageConversionsTimeMs)))
        m.tempMemoryBytesHist.foreach(_.update(temporaryMemoryBytes))
      }

      // Records network handler thread usage. This is included towards the request quota for the
      // user/client. Throttling is only performed when request handler thread usage
      // is recorded, just before responses are queued for delivery.
      // The time recorded here is the time spent on the network thread for receiving this request
      // and sending the response. Note that for the first request on a connection, the time includes
      // the total time spent on authentication, which may be significant for SASL/SSL.
      recordNetworkThreadTimeCallback.foreach(record => record(networkThreadTimeNanos))

      if (isRequestLoggingEnabled) {
        val desc = RequestConvertToJson.requestDescMetrics(header, requestLog, response.responseLog,
          context, session, isForwarded,
          totalTimeMs, requestQueueTimeMs, apiLocalTimeMs,
          apiRemoteTimeMs, apiThrottleTimeMs, responseQueueTimeMs,
          responseSendTimeMs, temporaryMemoryBytes,
          messageConversionsTimeMs)
        requestLogger.debug("Completed request:" + desc.toString)
      }
    }

    def releaseBuffer(): Unit = {
      envelope match {
        case Some(request) =>
          request.releaseBuffer()
        case None =>
          if (buffer != null) {
            memoryPool.release(buffer)
            buffer = null
          }
      }
    }

    override def toString = s"Request(processor=$processor, " +
      s"connectionId=${context.connectionId}, " +
      s"session=$session, " +
      s"listenerName=${context.listenerName}, " +
      s"securityProtocol=${context.securityProtocol}, " +
      s"buffer=$buffer, " +
      s"envelope=$envelope)"

  }

  /**
   * 定义Response的抽象基类。每个Response对象都包含了对应的Request对象。
   * 这个类里最重要的方法是onComplete方法，用来实现每类Response被处理后需要执行的回调逻辑
   * @param request 每个 Response 对象需要保存它对应的 Request对象
   */
  sealed abstract class Response(val request: Request) {

    def processor: Int = request.processor

    def responseLog: Option[JsonNode] = None

    def onComplete: Option[Send => Unit] = None
  }

  /** responseLogValue should only be defined if request logging is enabled */
  /**
   * 大多数Request处理完成都需要了执行一段回调逻辑，
   * SendResponse就是保存返回结果的Response子类。
   * 里面最重要的字段是onCompletionCallback, 即指定处理完成之后的回调逻辑
   * @param request
   * @param responseSend
   * @param responseLogValue
   * @param onCompleteCallback
   */
  class SendResponse(request: Request,
                     val responseSend: Send,
                     val responseLogValue: Option[JsonNode],
                     val onCompleteCallback: Option[Send => Unit]) extends Response(request) {
    override def responseLog: Option[JsonNode] = responseLogValue

    /**
     * 重新定义了onComplete方法，指定输入参数onCompleteCallback
     * @return
     */
    override def onComplete: Option[Send => Unit] = onCompleteCallback

    override def toString: String =
      s"Response(type=Send, request=$request, send=$responseSend, asString=$responseLogValue)"
  }

  /**
   * 有些Request处理完成后无需单独执行额外的回调逻辑。NoOpResponse就是为这类Response准备的
   * @param request
   */
  class NoOpResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=NoOp, request=$request)"
  }

  /**
   * 用于出错后需要关闭TCP连接的场景
   * 此时返回CloseConnectionResponse给Request发送方，显式地通知它关闭连接
   * @param request
   */
  class CloseConnectionResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=CloseConnection, request=$request)"
  }

  /**
   * 用于通知Broker的Socket Server组件 某个TCP连接通信通道开始被限流(throttling)
   * @param request
   */
  class StartThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=StartThrottling, request=$request)"
  }

  /**
   * 与{@link StartThrottlingResponse}相对应，通知Broker的Socket Server组件
   * 某个TCP连接通信通道的限流已结束
   * @param request
   */
  class EndThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=EndThrottling, request=$request)"
  }
}

/**
 * 一个不断接收外部请求、处理请求，然后发送处理结果的Java进程
 * 高效地保存排队中的请求，是确保Broker高性能处理的关键
 * Broker底层请求对象的建模 和 请求队列的实现原理 以及 Broker请求处理的核心监控指标
 *
 * 顾名思义，就是传输Request/Response的通道
 * @link KafkaMetricsGroup 封装了很多实用的指标监控方法, 比如， newGauge方法用于创建数值型的监控指标,
 *      newHistogram方法用户创建直方图型的监控指标
 * @param queueSize Request队列的最大长度。 当Broker启动时，SocketServer组件会创建{@link RequestChannel}对象
 *                  并把Broker参数queued.max.requests赋值给queueSize.因此默认情况下，每个RequestChannel上的队列长度为500
 * @param metricNamePrefix
 * @param time
 * @param metrics
 *
 * @variable requestQueue 定义一个队列来保存Broker接收到的各类请求，这个队列被称为请求队列或Request队列
 *        Kafka利用Java提供的阻塞队列{@link ArrayBlockingQueue}实现这个请求队列，并利用它天然的
 *        线程安全性来保证多个线程能够并发安全高效地访问请求队列
 * @variable processors 封装的是{@link RequestChannel}下辖的Processor线程池。
 *        每个Processor线程负责具体的请求处理逻辑。使用ConcurrentHashMap保存，Key是{@link kafka.network.RequestChannel.Request#processor}
 *        而Value则对应具体的Proceeor线程对象。
 *        这个线程池的存在说明：当前 Kafka Broker 端所有网络线程都是在RequestChannel中维护。
 *        {@link addProcessor()}和{@link removeProcessor()}方法来管理线程池
 */
class RequestChannel(val queueSize: Int,
                     val metricNamePrefix: String,
                     time: Time,
                     val metrics: RequestChannel.Metrics) extends KafkaMetricsGroup {
  import RequestChannel._
  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)
  private val processors = new ConcurrentHashMap[Int, Processor]()
  val requestQueueSizeMetricName = metricNamePrefix.concat(RequestQueueSizeMetric)
  val responseQueueSizeMetricName = metricNamePrefix.concat(ResponseQueueSizeMetric)

  newGauge(requestQueueSizeMetricName, () => requestQueue.size)

  newGauge(responseQueueSizeMetricName, () => {
    processors.values.asScala.foldLeft(0) {(total, processor) =>
      total + processor.responseQueueSize
    }
  })

  def addProcessor(processor: Processor): Unit = {
    // 添加Processor到Processor线程池
    if (processors.putIfAbsent(processor.id, processor) != null)
      warn(s"Unexpected processor with processorId ${processor.id}")

    // 为给定Processor对象创建对应的监控指标
    newGauge(responseQueueSizeMetricName, () => processor.responseQueueSize,
      Map(ProcessorMetricTag -> processor.id.toString))
  }

  def removeProcessor(processorId: Int): Unit = {
    processors.remove(processorId) // 从Processor线程池中移除给定的Processor线程
    removeMetric(responseQueueSizeMetricName, Map(ProcessorMetricTag -> processorId.toString)) // 移除对应Processor的监控指标
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  /**
   * 发送Request，仅仅是将Request对象放置在Request队列中而已
   * @param request
   */
  def sendRequest(request: RequestChannel.Request): Unit = {
    requestQueue.put(request)
  }

  def closeConnection(
    request: RequestChannel.Request,
    errorCounts: java.util.Map[Errors, Integer]
  ): Unit = {
    // This case is used when the request handler has encountered an error, but the client
    // does not expect a response (e.g. when produce request has acks set to 0)
    updateErrorMetrics(request.header.apiKey, errorCounts.asScala)
    sendResponse(new RequestChannel.CloseConnectionResponse(request))
  }

  def sendResponse(
    request: RequestChannel.Request,
    response: AbstractResponse,
    onComplete: Option[Send => Unit]
  ): Unit = {
    updateErrorMetrics(request.header.apiKey, response.errorCounts.asScala)
    sendResponse(new RequestChannel.SendResponse(
      request,
      request.buildResponseSend(response),
      request.responseNode(response),
      onComplete
    ))
  }

  def sendNoOpResponse(request: RequestChannel.Request): Unit = {
    sendResponse(new network.RequestChannel.NoOpResponse(request))
  }

  def startThrottling(request: RequestChannel.Request): Unit = {
    sendResponse(new RequestChannel.StartThrottlingResponse(request))
  }

  def endThrottling(request: RequestChannel.Request): Unit = {
    sendResponse(new EndThrottlingResponse(request))
  }

  /** Send a response back to the socket server to be sent over the network */
  /**
   * 把Response对象发送出去，也就是将Response添加到Response队列的过程
   * @param response
   */
  private[network] def sendResponse(response: RequestChannel.Response): Unit = {
    if (isTraceEnabled) { // 构造Trace日志输出字符串
      val requestHeader = response.request.headerForLoggingOrThrottling()
      val message = response match {
        case sendResponse: SendResponse =>
          s"Sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} of ${sendResponse.responseSend.size} bytes."
        case _: NoOpResponse =>
          s"Not sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} as it's not required."
        case _: CloseConnectionResponse =>
          s"Closing connection for client ${requestHeader.clientId} due to error during ${requestHeader.apiKey}."
        case _: StartThrottlingResponse =>
          s"Notifying channel throttling has started for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
        case _: EndThrottlingResponse =>
          s"Notifying channel throttling has ended for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
      }
      trace(message)
    }

    response match {
      // We should only send one of the following per request
        // 对 SendResponse、NoOpResponse、CloseConnectionResponse进行监控
      case _: SendResponse | _: NoOpResponse | _: CloseConnectionResponse =>
        val request = response.request
        val timeNanos = time.nanoseconds()
        request.responseCompleteTimeNanos = timeNanos
        if (request.apiLocalCompleteTimeNanos == -1L)
          request.apiLocalCompleteTimeNanos = timeNanos
      // For a given request, these may happen in addition to one in the previous section, skip updating the metrics
      case _: StartThrottlingResponse | _: EndThrottlingResponse => ()
    }

    // 找出response对应的processor线程，即request当初是由哪个Processor线程处理的
    val processor = processors.get(response.processor)
    // The processor may be null if it was shutdown. In this case, the connections
    // are closed, so the response is dropped.
    // 将response对象放置到对应Processo线程的Response队列中
    if (processor != null) {
      processor.enqueueResponse(response)
    }
  }

  /**
   * 接收Request则是从队列中取出Request. 整个流程构成了一个迷你版"生产者-消费者"模式
   * 然后依靠ArrayBlockingQueue的线程安全性来确保整个过程的线程安全
   */
  /** Get the next request or block until specified time has elapsed */
  def receiveRequest(timeout: Long): RequestChannel.BaseRequest =
    requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /** Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.BaseRequest =
    requestQueue.take()

  def updateErrorMetrics(apiKey: ApiKeys, errors: collection.Map[Errors, Integer]): Unit = {
    errors.forKeyValue { (error, count) =>
      metrics(apiKey.name).markErrorMeter(error, count)
    }
  }

  def clear(): Unit = {
    requestQueue.clear()
  }

  def shutdown(): Unit = {
    clear()
    metrics.close()
  }

  def sendShutdownRequest(): Unit = requestQueue.put(ShutdownRequest)

}

object RequestMetrics {
  val consumerFetchMetricName = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName = ApiKeys.FETCH.name + "Follower"

  val RequestsPerSec = "RequestsPerSec" // 每秒处理的Request数，用于评估Broker的繁忙状态
  /**
   * 计算Request在Request队列中的平均等待时间，单位是毫秒
   * 倘若Request在队列中的等待时间过长，通常需要增加后端I/O线程的数量来加快队列中Request的拿取速度
   */
  val RequestQueueTimeMs = "RequestQueueTimeMs"
  /**
   * 计算Request时机被处理的时间，单位是毫秒。
   * 一旦定位到这个监控项的值很多，就需要进一步要就Request被处理的逻辑了
   * 具体分析到底是哪一步消耗了过多的时间
   */
  val LocalTimeMs = "LocalTimeMs"
  /**
   * Kafka的读写请求（PRODUCE请求和FETCH请求）逻辑设计等待其他Broker操作的步骤。
   * 等待其他Broker完成指定逻辑的时间。
   * Kafka生成环境中设置acks=all的Producer程序发送消息掩饰高的主要原因，往往是Remote Time高
   */
  val RemoteTimeMs = "RemoteTimeMs"
  val ThrottleTimeMs = "ThrottleTimeMs"
  val ResponseQueueTimeMs = "ResponseQueueTimeMs"
  val ResponseSendTimeMs = "ResponseSendTimeMs"
  /**
   * 计算Request被处理的完整流程时间
   */
  val TotalTimeMs = "TotalTimeMs"
  val RequestBytes = "RequestBytes"
  val MessageConversionsTimeMs = "MessageConversionsTimeMs"
  val TemporaryMemoryBytes = "TemporaryMemoryBytes"
  val ErrorsPerSec = "ErrorsPerSec"
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {

  import RequestMetrics._

  val tags = Map("request" -> name)
  val requestRateInternal = new Pool[Short, Meter]()
  // time a request spent in a request queue
  val requestQueueTimeHist = newHistogram(RequestQueueTimeMs, biased = true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = newHistogram(LocalTimeMs, biased = true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram(RemoteTimeMs, biased = true, tags)
  // time a request is throttled, not part of the request processing time (throttling is done at the client level
  // for clients that support KIP-219 and by muting the channel for the rest)
  val throttleTimeHist = newHistogram(ThrottleTimeMs, biased = true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist = newHistogram(ResponseQueueTimeMs, biased = true, tags)
  // time to send the response to the requester
  val responseSendTimeHist = newHistogram(ResponseSendTimeMs, biased = true, tags)
  val totalTimeHist = newHistogram(TotalTimeMs, biased = true, tags)
  // request size in bytes
  val requestBytesHist = newHistogram(RequestBytes, biased = true, tags)
  // time for message conversions (only relevant to fetch and produce requests)
  val messageConversionsTimeHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(MessageConversionsTimeMs, biased = true, tags))
    else
      None
  // Temporary memory allocated for processing request (only populated for fetch and produce requests)
  // This shows the memory allocated for compression/conversions excluding the actual request size
  val tempMemoryBytesHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(TemporaryMemoryBytes, biased = true, tags))
    else
      None

  private val errorMeters = mutable.Map[Errors, ErrorMeter]()
  Errors.values.foreach(error => errorMeters.put(error, new ErrorMeter(name, error)))

  def requestRate(version: Short): Meter = {
    requestRateInternal.getAndMaybePut(version, newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags + ("version" -> version.toString)))
  }

  class ErrorMeter(name: String, error: Errors) {
    private val tags = Map("request" -> name, "error" -> error.name)

    @volatile private var meter: Meter = null

    def getOrCreateMeter(): Meter = {
      if (meter != null)
        meter
      else {
        synchronized {
          if (meter == null)
             meter = newMeter(ErrorsPerSec, "requests", TimeUnit.SECONDS, tags)
          meter
        }
      }
    }

    def removeMeter(): Unit = {
      synchronized {
        if (meter != null) {
          removeMetric(ErrorsPerSec, tags)
          meter = null
        }
      }
    }
  }

  def markErrorMeter(error: Errors, count: Int): Unit = {
    errorMeters(error).getOrCreateMeter().mark(count.toLong)
  }

  def removeMetrics(): Unit = {
    for (version <- requestRateInternal.keys) removeMetric(RequestsPerSec, tags + ("version" -> version.toString))
    removeMetric(RequestQueueTimeMs, tags)
    removeMetric(LocalTimeMs, tags)
    removeMetric(RemoteTimeMs, tags)
    removeMetric(RequestsPerSec, tags)
    removeMetric(ThrottleTimeMs, tags)
    removeMetric(ResponseQueueTimeMs, tags)
    removeMetric(TotalTimeMs, tags)
    removeMetric(ResponseSendTimeMs, tags)
    removeMetric(RequestBytes, tags)
    removeMetric(ResponseSendTimeMs, tags)
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name) {
      removeMetric(MessageConversionsTimeMs, tags)
      removeMetric(TemporaryMemoryBytes, tags)
    }
    errorMeters.values.foreach(_.removeMeter())
    errorMeters.clear()
  }
}
