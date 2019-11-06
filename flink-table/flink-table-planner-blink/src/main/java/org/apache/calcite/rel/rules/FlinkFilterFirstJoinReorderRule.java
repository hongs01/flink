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

package org.apache.calcite.rel.rules;

import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.util.Preconditions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static java.util.Comparator.comparing;

public class FlinkFilterFirstJoinReorderRule extends MultiJoinOptimizeBushyRule {

	public static final FlinkFilterFirstJoinReorderRule INSTANCE = new FlinkFilterFirstJoinReorderRule();

	private FlinkFilterFirstJoinReorderRule() {
		super(RelFactories.LOGICAL_BUILDER);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		final MultiJoin multiJoinRel = call.rel(0);
		if (multiJoinRel.getInputs().size() == 2) {
			Pair<JoinRelType, RexNode> joinTypeAndJoinCondition = analyzeBinaryMultiJoin(multiJoinRel);
			LogicalJoin join = LogicalJoin.create(
					multiJoinRel.getInput(0), multiJoinRel.getInput(1),
					joinTypeAndJoinCondition.right, ImmutableSet.of(), joinTypeAndJoinCondition.left);
			call.transformTo(join);
			return;
		}

		final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
		final RelBuilder relBuilder = call.builder();
		final FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(call.getMetadataQuery());

		final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);

		RelNode leftOrRightJoin = null;
		final List<Vertex> vertexes = new ArrayList<>();
		int x = 0;
		for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
			final RelNode rel = multiJoin.getJoinFactor(i);
			if (multiJoinRel.getJoinTypes().get(i) == JoinRelType.INNER) {
				Double filteredPercentage = fmq.getFilteredPercentage(rel);
				if (filteredPercentage == null) {
					filteredPercentage = 1.0;
				}
				vertexes.add(new LeafVertex(i, rel, filteredPercentage, x));
			} else {
				leftOrRightJoin = rel;
			}
			x += rel.getRowType().getFieldCount();
		}
		assert x == multiJoin.getNumTotalFields();

		final List<LoptMultiJoin.Edge> unusedEdges = new ArrayList<>();
		for (RexNode node : multiJoin.getJoinFilters()) {
			LoptMultiJoin.Edge edge = multiJoin.createEdge(node);
			if (!hasLeftOrRightJoinFilter(edge, multiJoinRel)) {
				unusedEdges.add(edge);
			}
		}

		// Comparator that chooses the best edge. A "good edge" is one that has
		// a large difference in the number of rows on LHS and RHS.
		final Comparator<LoptMultiJoin.Edge> edgeComparator = (e0, e1) -> {
			assert e0.factors.cardinality() == 2 : e0.factors;
			assert e1.factors.cardinality() == 2 : e1.factors;

			final int factor0 = e0.factors.nextSetBit(0);
			final int factor1 = e0.factors.nextSetBit(factor0 + 1);

			final int factor2 = e1.factors.nextSetBit(0);
			final int factor3 = e1.factors.nextSetBit(factor2 + 1);

			double costE0 = vertexes.get(factor0).cost + vertexes.get(factor1).cost;
			double costE1 = vertexes.get(factor2).cost + vertexes.get(factor3).cost;

			int c = Double.compare(costE0, costE1);
			if (c == 0) {
				// try to maintain the original join order ???
				c = Integer.compare(factor0 + factor1, factor2 + factor3);
			}
			return c;
		};

		Collections.sort(unusedEdges, edgeComparator);

		final List<LoptMultiJoin.Edge> usedEdges = new ArrayList<>();
		for (; ; ) {
			final int[] factors;
			if (unusedEdges.isEmpty()) {
				// No more edges. Are there any un-joined vertexes?
				final Vertex lastVertex = Util.last(vertexes);
				final int z = lastVertex.factors.previousClearBit(lastVertex.id - 1);
				if (z < 0) {
					break;
				}
				factors = new int[] { z, lastVertex.id };
			} else {
				final LoptMultiJoin.Edge bestEdge = unusedEdges.get(0);

				// For now, assume that the edge is between precisely two factors.
				// 1-factor conditions have probably been pushed down,
				// and 3-or-more-factor conditions are advanced. (TODO:)
				// Therefore, for now, the factors that are merged are exactly the
				// factors on this edge.
				assert bestEdge.factors.cardinality() == 2;
				factors = bestEdge.factors.toArray();
			}

			// try to maintain the original join order
			final int majorFactor = Math.min(factors[0], factors[1]);
			final int minorFactor = Math.max(factors[0], factors[1]);
			final Vertex majorVertex = vertexes.get(majorFactor);
			final Vertex minorVertex = vertexes.get(minorFactor);

			// Find the join conditions. All conditions whose factors are now all in
			// the join can now be used.
			final int v = vertexes.size();
			final ImmutableBitSet newFactors =
					majorVertex.factors
							.rebuild()
							.addAll(minorVertex.factors)
							.set(v)
							.build();

			final List<RexNode> conditions = new ArrayList<>();
			final Iterator<LoptMultiJoin.Edge> edgeIterator = unusedEdges.iterator();
			while (edgeIterator.hasNext()) {
				LoptMultiJoin.Edge edge = edgeIterator.next();
				if (newFactors.contains(edge.factors)) {
					conditions.add(edge.condition);
					edgeIterator.remove();
					usedEdges.add(edge);
				}
			}

			double cost = majorVertex.cost + minorVertex.cost;
			final Vertex newVertex =
					new JoinVertex(v, majorFactor, minorFactor, newFactors, cost, ImmutableList.copyOf(conditions));
			vertexes.add(newVertex);

			// Re-compute selectivity of edges above the one just chosen.
			// Suppose that we just chose the edge between "product" (10k rows) and
			// "product_class" (10 rows).
			// Both of those vertices are now replaced by a new vertex "P-PC".
			// This vertex has fewer rows (1k rows) -- a fact that is critical to
			// decisions made later. (Hence "greedy" algorithm not "simple".)
			// The adjacent edges are modified.
			final ImmutableBitSet merged = ImmutableBitSet.of(minorFactor, majorFactor);
			for (int i = 0; i < unusedEdges.size(); i++) {
				final LoptMultiJoin.Edge edge = unusedEdges.get(i);
				if (edge.factors.intersects(merged)) {
					ImmutableBitSet newEdgeFactors =
							edge.factors
									.rebuild()
									.removeAll(newFactors)
									.set(v)
									.build();
					assert newEdgeFactors.cardinality() == 2;
					final LoptMultiJoin.Edge newEdge =
							new LoptMultiJoin.Edge(edge.condition, newEdgeFactors, edge.columns);
					unusedEdges.set(i, newEdge);
				}
			}
		}

		if (leftOrRightJoin != null) {
			int outerJoinIdx = multiJoin.getNumJoinFactors() - 1;
			Vertex lastVertex = Util.last(vertexes);
			int v = vertexes.size();
			Vertex leftOrRightJoinVertex = new LeafVertex(
					v, leftOrRightJoin, 1.0, x - leftOrRightJoin.getRowType().getFieldCount());
			vertexes.add(leftOrRightJoinVertex);

			JoinRelType joinType = multiJoinRel.getJoinTypes().get(outerJoinIdx);
			int majorFactor = lastVertex.id;
			int minorFactor = leftOrRightJoinVertex.id;
			int v1 = vertexes.size();
			ImmutableBitSet newFactors = lastVertex.factors.union(ImmutableBitSet.of(v, v1));
			Vertex joinVertex = new JoinVertex(
					v1, joinType, majorFactor, minorFactor, newFactors, Double.MAX_VALUE,
					ImmutableList.of(multiJoin.getOuterJoinCond(outerJoinIdx)));
			vertexes.add(joinVertex);
		}

		// We have a winner!
		List<Pair<RelNode, Mappings.TargetMapping>> relNodes = new ArrayList<>();
		for (Vertex vertex : vertexes) {
			if (vertex instanceof LeafVertex) {
				LeafVertex leafVertex = (LeafVertex) vertex;
				final Mappings.TargetMapping mapping =
						Mappings.offsetSource(
								Mappings.createIdentity(
										leafVertex.rel.getRowType().getFieldCount()),
								leafVertex.fieldOffset,
								multiJoin.getNumTotalFields());
				relNodes.add(Pair.of(leafVertex.rel, mapping));
			} else {
				JoinVertex joinVertex = (JoinVertex) vertex;
				final Pair<RelNode, Mappings.TargetMapping> leftPair =
						relNodes.get(joinVertex.leftFactor);
				RelNode left = leftPair.left;
				final Mappings.TargetMapping leftMapping = leftPair.right;
				final Pair<RelNode, Mappings.TargetMapping> rightPair =
						relNodes.get(joinVertex.rightFactor);
				RelNode right = rightPair.left;
				final Mappings.TargetMapping rightMapping = rightPair.right;
				final Mappings.TargetMapping mapping =
						Mappings.merge(leftMapping,
								Mappings.offsetTarget(rightMapping,
										left.getRowType().getFieldCount()));
				final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle(mapping, left, right);
				final RexNode condition = RexUtil.composeConjunction(rexBuilder, joinVertex.conditions, false);

				final RelNode join = relBuilder.push(left)
						.push(right)
						.join(joinVertex.joinType, condition.accept(shuttle))
						.build();
				relNodes.add(Pair.of(join, mapping));
			}
		}

		final Pair<RelNode, Mappings.TargetMapping> top = Util.last(relNodes);
		relBuilder.push(top.left).project(relBuilder.fields(top.right));
		call.transformTo(relBuilder.build());
	}

	private boolean hasLeftOrRightJoinFilter(LoptMultiJoin.Edge edge, MultiJoin multiJoin) {
		int numOfInputs = multiJoin.getInputs().size();
		if (multiJoin.getJoinTypes().get(numOfInputs - 1) != JoinRelType.INNER) {
			return edge.factors.get(numOfInputs - 1);
		} else {
			return false;
		}
	}

	private Pair<JoinRelType, RexNode> analyzeBinaryMultiJoin(MultiJoin multiJoin) {
		Preconditions.checkArgument(multiJoin.getInputs().size() == 2);
		JoinRelType joinType;
		RexNode joinCondition;
		if (multiJoin.isFullOuterJoin()) {
			joinType = JoinRelType.FULL;
			joinCondition = multiJoin.getJoinFilter();
		} else {
			if (multiJoin.getJoinTypes().get(1) == JoinRelType.LEFT) {
				joinType = JoinRelType.LEFT;
				joinCondition = multiJoin.getOuterJoinConditions().get(1);
			} else if (multiJoin.getJoinTypes().get(0) == JoinRelType.RIGHT) {
				joinType = JoinRelType.RIGHT;
				joinCondition = multiJoin.getOuterJoinConditions().get(0);
			} else {
				joinType = JoinRelType.INNER;
				joinCondition = multiJoin.getJoinFilter();
			}
		}
		return new Pair<>(joinType, joinCondition);
	}
}
