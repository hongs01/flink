/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.metadata

import org.apache.flink.table.planner.JDouble

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Filter, Project, TableScan}
import org.apache.calcite.rel.metadata._
import org.apache.calcite.util.Util

/**
  * FlinkRelMdPercentageOriginalRows supplies a implementation of
  * [[FlinkRelMetadataQuery#getFilteredPercentage]] for the standard logical algebra.
  */
class FlinkRelMdFilteredPercentage private
  extends MetadataHandler[FlinkMetadata.FilteredPercentage] {

  def getDef: MetadataDef[FlinkMetadata.FilteredPercentage] = FlinkMetadata.FilteredPercentage.DEF

  def getFilteredPercentage(rel: TableScan, mq: RelMetadataQuery): JDouble = 1.0

  def getFilteredPercentage(rel: Filter, mq: RelMetadataQuery): JDouble = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val inputPercentage = fmq.getFilteredPercentage(rel.getInput)
    if (inputPercentage != null) {
      // apply predicate on filter's input
      val selectivity = mq.getSelectivity(rel.getInput, rel.getCondition)
      if (selectivity != null) {
        return inputPercentage * selectivity
      }
    }
    null
  }

  def getFilteredPercentage(rel: Project, mq: RelMetadataQuery): JDouble = {
    mq.getPercentageOriginalRows(rel.getInput)
  }

  def getFilteredPercentage(subset: RelSubset, mq: RelMetadataQuery): JDouble = {
    val rel = Util.first(subset.getBest, subset.getOriginal)
    mq.getPercentageOriginalRows(rel)
  }

  def getFilteredPercentage(hepRelVertex: HepRelVertex, mq: RelMetadataQuery): JDouble = {
    mq.getPercentageOriginalRows(hepRelVertex.getCurrentRel)
  }

  /**
    * Catch-all rule when none of the others apply.
    */
  def getFilteredPercentage(rel: RelNode, mq: RelMetadataQuery): JDouble = null
}

object FlinkRelMdFilteredPercentage {

  private val INSTANCE = new FlinkRelMdFilteredPercentage

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.FilteredPercentage.METHOD, INSTANCE)

}
