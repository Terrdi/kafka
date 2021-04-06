/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.cluster

import org.apache.kafka.common.{Endpoint => JEndpoint, KafkaException}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils

import scala.collection.Map

object EndPoint {

  private val uriParseExp = """^(.*)://\[?([0-9a-zA-Z\-%._:]*)\]?:(-?[0-9]+)""".r

  private[kafka] val DefaultSecurityProtocolMap: Map[ListenerName, SecurityProtocol] =
    SecurityProtocol.values.map(sp => ListenerName.forSecurityProtocol(sp) -> sp).toMap

  /**
   * Create EndPoint object from `connectionString` and optional `securityProtocolMap`. If the latter is not provided,
   * we fallback to the default behaviour where listener names are the same as security protocols.
   *
   * @param connectionString the format is listener_name://host:port or listener_name://[ipv6 host]:port
   *                         for example: PLAINTEXT://myhost:9092, CLIENT://myhost:9092 or REPLICATION://[::1]:9092
   *                         Host can be empty (PLAINTEXT://:9092) in which case we'll bind to default interface
   *                         Negative ports are also accepted, since they are used in some unit tests
   */
  def createEndPoint(connectionString: String, securityProtocolMap: Option[Map[ListenerName, SecurityProtocol]]): EndPoint = {
    val protocolMap = securityProtocolMap.getOrElse(DefaultSecurityProtocolMap)

    def securityProtocol(listenerName: ListenerName): SecurityProtocol =
      protocolMap.getOrElse(listenerName,
        throw new IllegalArgumentException(s"No security protocol defined for listener ${listenerName.value}"))

    connectionString match {
      case uriParseExp(listenerNameString, "", port) =>
        val listenerName = ListenerName.normalised(listenerNameString)
        new EndPoint(null, port.toInt, listenerName, securityProtocol(listenerName))
      case uriParseExp(listenerNameString, host, port) =>
        val listenerName = ListenerName.normalised(listenerNameString)
        new EndPoint(host, port.toInt, listenerName, securityProtocol(listenerName))
      case _ => throw new KafkaException(s"Unable to parse $connectionString to a broker endpoint")
    }
  }
}

/**
 * Part of the broker definition - matching host/port pair to a protocol
 * Broker 端参数 listeners 和 advertised.listeners 是用来配置监听器的
 *
 * @param host Broker 主机名
 * @param port Broker 端口号
 * @param listenerName 监听器名称， 目前预定义的名称包括 PLAINTEXT、SSL、SASL_PLAINTEXT和SASL_SSL。
 *                     Kafka 允许自定义其他监听器名称, 比如 CONTROLLER、INTERNAL等
 * @param securityProtocol 监听器使用的安全协议。 Kafka 支持4中安全协议，分别是 PLAINTEXT、SSL、SASL_PLAINTEXT和SASL_SSL
 *                         Broker 端参数 listener.security.protocol.map 用于指定不同名字的监听器都使用哪种安全协议
 * for example:
 * listener.security.protocol.map = CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:SSL
 * listeners = CONTROLLER://192.1.1.8:9091,INTERNAL://192.1.1.8:9092,EXTERNAL://10.1.1.5:9093
 * 表示 Kafka配置了3套监听器，名字分别是 CONTROLLER, INTERNAL, EXTERNAL
 *                使用的安全协议分别是 PLAINTEXT, PLAINTEXT, SSL
 */
case class EndPoint(host: String, port: Int, listenerName: ListenerName, securityProtocol: SecurityProtocol) {
  // 构造完整的监听器连接字符串
  // 格式为: 监听器名称://主机名:端口
  // 比如: PLAINTEXT://kafka-host:9092
  def connectionString: String = {
    val hostport =
      if (host == null)
        ":"+port
      else
        Utils.formatAddress(host, port)
    listenerName.value + "://" + hostport
  }

  // clients 工程下有一个 Java 版本的Endpoint 类供 clients 端代码使用
  // 此方法是构造 Java 版本的 Endpoint 类实例
  def toJava: JEndpoint = {
    new JEndpoint(listenerName.value, securityProtocol, host, port)
  }
}
