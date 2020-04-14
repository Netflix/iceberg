/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.netflix.iceberg.batch;

import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;

class ScalaUtil {
  @SuppressWarnings("unchecked")
  static <K, V> Map<K, V> asScala(java.util.Map<K, V> map) {
    scala.collection.Map<K, V> scalaMap = JavaConverters.mapAsScalaMapConverter(map).asScala();
    return (Map<K, V>) Map$.MODULE$.apply(scalaMap.toSeq());
  }

  static <I> java.util.List<I> asJava(Seq<I> seq) {
    return JavaConverters.seqAsJavaListConverter(seq).asJava();
  }

  static <I> Seq<I> asScala(java.util.List<I> list) {
    return JavaConverters.asScalaBufferConverter(list).asScala();
  }

  static <I> java.util.Iterator<I> asJava(Iterator<I> scalaIter) {
    return JavaConverters.asJavaIteratorConverter(scalaIter).asJava();
  }

  static <I> Iterator<I> asScala(java.util.Iterator<I> iter) {
    return JavaConverters.asScalaIteratorConverter(iter).asScala();
  }

  @SuppressWarnings("unchecked")
  static <T> Seq<T> nil() {
    return (Seq<T>) Seq$.MODULE$.empty();
  }
}
