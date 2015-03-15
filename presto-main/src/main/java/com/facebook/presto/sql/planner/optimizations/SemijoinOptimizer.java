/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.optimizations;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.estimater.CostEstimater;
import com.facebook.presto.sql.planner.optimizations.estimater.LouLoader;
import com.facebook.presto.sql.planner.optimizations.estimater.SemijoinMetadata;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Optional;

public class SemijoinOptimizer extends PlanOptimizer{

	private final Metadata metadata;
	
	private Map<PlanNodeId, SemijoinMetadata> semijoinMetadata;
	
	private List<PlanNode> joinSources;
	
	private List<JoinNode> joinNodes;
	
	private Map<PlanNodeId, List<Symbol>> projections;
	
	private Map<String, Double> louMap;
	
	//TODO 1.删除semijoin重写部分，2.join order
	
	public SemijoinOptimizer(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        semijoinMetadata = new HashMap<PlanNodeId, SemijoinMetadata>();
        joinSources = new ArrayList<PlanNode>();
        joinNodes = new ArrayList<JoinNode>();
        projections = new HashMap<PlanNodeId, List<Symbol>>();
        louMap = LouLoader.getLouMap();
    }
	
	@Override
	public PlanNode optimize(PlanNode plan, Session session,
			Map<Symbol, Type> types, SymbolAllocator symbolAllocator,
			PlanNodeIdAllocator idAllocator) {
		
		//1.每个join尝试进行semijoin规约
		//TODO rewrite join order
		this.semijoinMetadata.clear();
		this.joinSources.clear();
		this.joinNodes.clear();
		PlanNode node = PlanRewriter.rewriteWith(new TableScanRewriter(metadata, session, idAllocator, semijoinMetadata, joinSources, joinNodes, projections), plan);
		this.estimateCost(node);
		//semijoin优化(presto不支持)
//		node = PlanRewriter.rewriteWith(new JoinRewriter(semijoinMetadata, louMap, joinSources, joinNodes, idAllocator, symbolAllocator, metadata), node);
		//join order 优化
		if(joinSources.size() > 2)
		node = PlanRewriter.rewriteWith(new JoinOrderRewriter(semijoinMetadata, joinSources, joinNodes, projections, idAllocator, symbolAllocator, metadata), node);
		//重写output与project
		return node;
		
	}
	
//	private PlanNode optimizeJoinOrder(PlanNode node, PlanNodeIdAllocator idAllocator){
//		if(joinSources.size() > 2)
//		{
//			joinSources = orderArray(joinSources);
//			for(int i = 0; i < joinSources.size() - 2; i++)
//			{
//				PlanNode source = joinSources.get(i);
//				JoinNode join = (JoinNode)getSon(node, source);
//				List<EquiJoinClause> criteria = getCriteriaFromEquiJoinClause(join.getCriteria(), source.getOutputSymbols()); 
//				JoinNode newJoin = new JoinNode(idAllocator.getNextId(), join.getType(), join.getLeft(), source, criteria, join.getLeftHashSymbol(), join.getRightHashSymbol());
//				
//				
//			}
//		}
//		return node;
//	}

	
	
	
	private Long estimateCost(PlanNode node){
		if(node.getSources() != null && node.getSources().size() == 1)
		{
			Long cost = this.estimateCost(node.getSources().get(0));
			if(cost == null)
				return null;
			else if(node instanceof FilterNode)
			{
				FilterNode filterNode = (FilterNode) node;
				double selectivity = CostEstimater.selectivity(filterNode.getPredicate());
				cost = new Double(cost * selectivity).longValue();
			}
			SemijoinMetadata meta = new SemijoinMetadata();
			meta.setRowCount(cost);
			semijoinMetadata.put(node.getId(), meta);
			return cost;
		}
		else if(node.getSources().size() == 2)
		{
			this.estimateCost(node.getSources().get(0));
			this.estimateCost(node.getSources().get(1));
			return null;
		}
		else
			return semijoinMetadata.get(node.getId()).getRowCount();
	}
	
	/**
	 * 重写tablescannode
	 * @author lin
	 *
	 */
	private static class JoinOrderRewriter extends PlanNodeRewriter<Void>
	{
		private Map<PlanNodeId, SemijoinMetadata> semijoinMetadata;
		private Metadata metadata;
		private List<PlanNode> joinSources;
		private List<JoinNode> joinNodes;
		private Map<PlanNodeId, List<Symbol>> projections;
		private final PlanNodeIdAllocator idAllocator;
		private final SymbolAllocator symbolAllocator;
		private int index = 0;
		
		private JoinOrderRewriter(Map<PlanNodeId, SemijoinMetadata> semijoinMetadata, List<PlanNode> joinSources, List<JoinNode> joinNodes,Map<PlanNodeId, List<Symbol>> projections, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Metadata metadata){
			this.semijoinMetadata = semijoinMetadata;
			this.joinSources = orderArray(joinSources);
			this.joinNodes = joinNodes;
			this.projections = projections;
			this.idAllocator = idAllocator;
			this.symbolAllocator = symbolAllocator;
			this.metadata = metadata;
			
		}


		private Long getRowCount(PlanNode node){
			return semijoinMetadata.get(node.getId()).getRowCount();
		}

		private List<PlanNode> orderArray(List<PlanNode> list){
			List<PlanNode> copy = new ArrayList<PlanNode>(list.size());
			for(PlanNode node : list)
				copy.add(node);
			List<PlanNode> orderList = new ArrayList<PlanNode>();
			for(int i = 0; i < list.size(); i++)
			{
				PlanNode node = copy.get(0);
				for(int j = 1; j < copy.size(); j++)
				{
					PlanNode ins = copy.get(j);
					if(getRowCount(ins) < getRowCount(node))
						node = ins;
				}
				copy.remove(node);
				orderList.add(node);
			}
			return orderList;
		}
		
		@Override
		public PlanNode rewriteTableScan(TableScanNode node, Void context,
				PlanRewriter<Void> planRewriter) {
			TableScanNode newTableScan = getSourceTable(joinSources.get(index++));
			if(newTableScan.getId().equals(node.getId()))
				return super.rewriteTableScan(node, context, planRewriter);
			Map<Symbol, ColumnHandle> assignments = new HashMap<Symbol, ColumnHandle>();
			for(Symbol symbol : node.getAssignments().keySet())
			{
				for(ColumnHandle column : metadata.getColumnHandles(newTableScan.getTable()).values())
				{
					String columnName = metadata.getColumnMetadata(newTableScan.getTable(), column).getName();
					if(columnName.equals(getSymbolKey(symbol)))
					{
						assignments.put(symbol, column);
						break;
					}
				}
			}
			return new TableScanNode(idAllocator.getNextId(), newTableScan.getTable(), node.getOutputSymbols(), assignments, newTableScan.getOriginalConstraint());
		}
		
//		@Override
//		public PlanNode rewriteJoin(JoinNode node, Void context,
//				PlanRewriter<Void> planRewriter) {
//			if(!isContain(joinNodes, node))
//				return super.rewriteJoin(node, context, planRewriter);
//			//重写中间节点
//			if(index < (joinSources.size() - 2))
//			{
//				PlanNode newRight = joinSources.get(index++);
//				PlanNode newLeft = this.rewriteSource(node.getLeft(), context, planRewriter);
//				List<EquiJoinClause> criteria = getCriteria(newLeft.getOutputSymbols(), newRight.getOutputSymbols());
//				return new JoinNode(idAllocator.getNextId(), node.getType(), newLeft, newRight, criteria, node.getLeftHashSymbol(), node.getRightHashSymbol());
//			}
//			//重写最后节点
//			else
//			{
//				PlanNode newRight = joinSources.get(index++);
//				PlanNode newLeft = joinSources.get(index);
//				List<EquiJoinClause> criteria = getCriteria(newLeft.getOutputSymbols(), newRight.getOutputSymbols());
//				return new JoinNode(idAllocator.getNextId(), node.getType(), newLeft, newRight, criteria, node.getLeftHashSymbol(), node.getRightHashSymbol());
//			}
//		}
		
		private PlanNode rewriteSource(PlanNode source, Void context,
				PlanRewriter<Void> planRewriter){
			if(source instanceof JoinNode)
				return rewriteJoin((JoinNode)source, context, planRewriter);
			PlanNode newSource = rewriteSource(source.getSources().get(0), context, planRewriter);
			if(source instanceof FilterNode)
				return copyFilterNode((FilterNode)source, newSource, idAllocator);
			else if(source instanceof ProjectNode)
			{
				Map<Symbol, Expression> assignment = new HashMap<Symbol, Expression>();
				List<PlanNode> joinParts = newSource.getSources();
				for(PlanNode joinPart : joinParts)
				{
					if(!(joinPart instanceof TableScanNode))
						continue;
					assignment.putAll(getAssignments(projections.get(joinPart.getId()), symbolAllocator));
				}
				return new ProjectNode(idAllocator.getNextId(), newSource, assignment);
			}
			else
				return null;
		}
	}
	
	/**
	 * 半连接优化
	 * 测试：全部等值连接，全部可重排序
	 * @author lin
	 *
	 */
	private static class JoinRewriter extends PlanNodeRewriter<Void>{
		private Map<PlanNodeId, SemijoinMetadata> semijoinMetadata;
		private Map<String, Double> louMap;
		private Metadata metadata;
		private List<PlanNode> joinSources;
		private List<JoinNode> joinNodes;
		private final PlanNodeIdAllocator idAllocator;
		private final SymbolAllocator symbolAllocator;
		
		
		private JoinRewriter(Map<PlanNodeId, SemijoinMetadata> semijoinMetadata, Map<String, Double> louMap, List<PlanNode> joinSources, List<JoinNode> joinNodes, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Metadata metadata){
			this.semijoinMetadata = semijoinMetadata;
			this.louMap = louMap;
			this.joinSources = joinSources;
			this.joinNodes = joinNodes;
			this.idAllocator = idAllocator;
			this.symbolAllocator = symbolAllocator;
			this.metadata = metadata;
		}
		
		public PlanNode rewriteJoin(JoinNode node, Void context, com.facebook.presto.sql.planner.plan.PlanRewriter<Void> planRewriter) {
			PlanNode part = node.getLeft();
			PlanNode bestFilter = null;
			double bestProfit = 0;
			JoinNode newJoinNode = null;
			for(PlanNode filterNode : joinSources)
			{
				if(part.getId() == filterNode.getId())
					continue;
				double profit = this.calculateProfit(part, filterNode, node.getCriteria());
				if(profit > bestProfit)
				{
					bestProfit = profit;
					bestFilter = filterNode;
				}
			}
			if(bestFilter != null)
			{
				PlanNode newFilter = copyNode(bestFilter, idAllocator, symbolAllocator);
				List<Symbol> outputSymbol = new ArrayList<Symbol>();
				List<Symbol> joinSymbol = new ArrayList<Symbol>();
				for(EquiJoinClause clause : node.getCriteria())
				{
					outputSymbol.add(clause.getRight());
					joinSymbol.add(clause.getLeft());
				}
				if(!isSymbolEqual(newFilter.getOutputSymbols(), outputSymbol))
				{
					//如果是tablescan直接重写，否则在filter上添加project
					if(newFilter instanceof TableScanNode)
					{
						Map<Symbol, ColumnHandle> assignments = new HashMap<Symbol, ColumnHandle>();
						for(Symbol instance : outputSymbol)
						{
							Symbol symbol = getEqualSymbol(instance, newFilter.getOutputSymbols());
							Symbol newSymbol = symbolAllocator.newSymbol(getSymbolKey(symbol), symbolAllocator.getTypes().get(symbol));
							assignments.put(newSymbol, ((TableScanNode)newFilter).getAssignments().get(symbol));
						}
						newFilter = new TableScanNode(idAllocator.getNextId(), ((TableScanNode) newFilter).getTable(), new ArrayList(assignments.keySet()), assignments, ((TableScanNode) newFilter).getOriginalConstraint());
					}
					else
					{
						Map<Symbol, Expression> assignments = new HashMap<Symbol, Expression>();
						for(Symbol instance : outputSymbol)
						{
							Symbol symbol = getEqualSymbol(instance, newFilter.getOutputSymbols());
							Symbol newSymbol = symbolAllocator.newSymbol(getSymbolKey(symbol), symbolAllocator.getTypes().get(symbol));
							assignments.put(newSymbol, new QualifiedNameReference(symbol.toQualifiedName()));
						}
					newFilter = new ProjectNode(idAllocator.getNextId(), newFilter, assignments);
					}
				}
				List<EquiJoinClause> criteria = getCriteria(joinSymbol, newFilter.getOutputSymbols());
				JoinNode newNode = new JoinNode(idAllocator.getNextId(), JoinNode.Type.INNER, part, newFilter, criteria, Optional.absent(), Optional.absent());
				newJoinNode = new JoinNode(idAllocator.getNextId(), JoinNode.Type.INNER, newNode, node.getRight(), node.getCriteria(), node.getLeftHashSymbol(), node.getRightHashSymbol());
				//更新join列表与费用
				joinSources.remove(part);
				joinSources.add(newNode);
				SemijoinMetadata newMeta = new SemijoinMetadata();
				newMeta.setRowCount(new Double(semijoinMetadata.get(part.getId()).getRowCount() * this.getLou(bestFilter, metadata)).longValue());
				semijoinMetadata.put(newNode.getId(), newMeta);
			}
			//判断最优semijoin并改写为semijoinnode
			joinNodes.add(newJoinNode);
			return newJoinNode;
		};
		
		
		private double calculateProfit(PlanNode sourceNode, PlanNode filterNode, List<EquiJoinClause> joinClause){
			TableScanNode sourceTable = getSourceTable(sourceNode);
			
			SemijoinMetadata sourceMeta = semijoinMetadata.get(sourceTable.getId());
			List<Symbol> symbols = sourceNode.getOutputSymbols();
			Integer sourceWidth = 0;
			for(Symbol symbol : symbols)
			{
				Integer width = null;
				if((width = sourceMeta.getColumnWidth(symbol.getName())) != null)
					sourceWidth += width;
			}
			Integer filterWidth = 0;
			for(EquiJoinClause clause : joinClause)
				filterWidth += sourceMeta.getColumnWidth(clause.getLeft().getName());
			double lou = this.getLou(filterNode, metadata);
			double cost = filterWidth * semijoinMetadata.get(filterNode.getId()).getRowCount();
			double benefit = (1 - lou) * sourceWidth * semijoinMetadata.get(sourceNode.getId()).getRowCount();
			return benefit - cost;		
		}
		
		private double getLou(PlanNode node, Metadata metadata){
			TableScanNode filterTable = getSourceTable(node);
			String fullTableName = getFullTableName(filterTable, metadata);//TODO debug
			return louMap.get(fullTableName);
		}

	}
	
	/**
	 * 估算行数
	 * @author lin
	 *
	 */
	private static class TableScanRewriter extends PlanNodeRewriter<Void>{
		
		private TableScanRewriter(Metadata metadata, Session session, PlanNodeIdAllocator idAllocator, Map<PlanNodeId, SemijoinMetadata> semijoinMetadata, List<PlanNode> joinSources, List<JoinNode> joinNodes, Map<PlanNodeId, List<Symbol>> projections){
			this.metadata = checkNotNull(metadata, "metadata is null");
			this.idAllocator = checkNotNull(idAllocator, "idAllocator is null");
			this.session = checkNotNull(session, "session is null");
			this.semijoinMetadata = semijoinMetadata;
			this.joinSources = joinSources;
			this.joinNodes = joinNodes;
			this.projections = projections;
		}

		private Map<PlanNodeId, SemijoinMetadata> semijoinMetadata;
		
		private List<EquiJoinClause> lastJoinClauses;
		
		private List<JoinNode> joinNodes;
		
		private List<PlanNode> joinSources;
		
		private Map<PlanNodeId, List<Symbol>> projections;
		
		private final Metadata metadata;
		
		private final Session session;
		
		@SuppressWarnings("unused")
		private final PlanNodeIdAllocator idAllocator;
		
		@Override
		public PlanNode rewriteProject(ProjectNode node, Void context,
				PlanRewriter<Void> planRewriter) {
			if(!(node.getSource() instanceof JoinNode))
				return super.rewriteProject(node, context, planRewriter);
			List<QualifiedNameReference> expressions = new ArrayList(node.getAssignments().values());
			List<PlanNode> joinParts = node.getSource().getSources();
			for(PlanNode joinPart : joinParts)
			{
				List<Symbol> projectList = new ArrayList<Symbol>();
				if(!(joinPart instanceof TableScanNode))
					continue;
				List<Symbol> outputs = joinPart.getOutputSymbols();
				for(Symbol output : outputs)
				{
					for(QualifiedNameReference expression : expressions)
					{
						if(output.equals(Symbol.fromQualifiedName(expression.getName())))
						{
							projectList.add(output);
							break;
						}
					}
				}
				projections.put(joinPart.getId(), projectList);
			}
			return super.rewriteProject(node, context, planRewriter);
		}
		
		@Override
		public PlanNode rewriteJoin(JoinNode node, Void context, com.facebook.presto.sql.planner.plan.PlanRewriter<Void> planRewriter) {
			if(node.getType() == JoinNode.Type.INNER)
			{
				joinNodes.add(node);
				if(lastJoinClauses != null)
					if(!isContain(lastJoinClauses, node.getCriteria()))
						joinSources.clear();
				if(!isJoinNode(node.getLeft()))
					joinSources.add(node.getLeft());
				if(!isJoinNode(node.getRight()))
					joinSources.add(node.getRight());
			}
			return super.rewriteJoin(node, context, planRewriter);
		};
		
//		@Override
//		public PlanNode rewriteJoin(JoinNode node, Void context,
//				PlanRewriter<Void> planRewriter) {
//			//step 1: get row count of each side;
//			TableScanNode b = null;
//			b.getTable().getConnectorHandle();
//			MetadataUtil.createQualifiedTableName(session, table.getName());
//			metadata.getRowCount(session, viewName);
//			String connectorId = b.getTable().getConnectorId();
//			ConnectorSession cSession = session.toConnectorSession(connectorId);
//			Map<String, String> values = cSession.getProperties();
//			
//			//not suitable for outer join
//			if(node.getType() == JoinNode.Type.CROSS)
//				return node;
//			
//			//get symbols
//			List<JoinNode.EquiJoinClause> clauses = node.getCriteria();
//			List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
//            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);
//			
//			PlanNode leftRewritten = planRewriter.rewrite(node.getLeft(), context);
//            PlanNode rightRewritten = planRewriter.rewrite(node.getRight(), context);
//            PlanNode copy = this.copyPlanNode(leftRewritten);
//            Symbol leftSymbol = node.getCriteria().get(0).getLeft();
//            ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
//            Expression expression = new QualifiedNameReference(leftSymbol.toQualifiedName());
//            projections.put(leftSymbol, expression);
//			ProjectNode leftProject = new ProjectNode(idAllocator.getNextId(), copy, projections.build());
//			JoinNode leftSemiJoin = new JoinNode(idAllocator.getNextId(), JoinNode.Type.INNER, leftProject, rightRewritten, node.getCriteria(), Optional.<Symbol>absent(), Optional.<Symbol>absent());
//			ProjectNode rightProject = new ProjectNode(idAllocator.getNextId(), leftSemiJoin, projections.build());
//			JoinNode rightSemiJoin = new JoinNode(idAllocator.getNextId(), JoinNode.Type.INNER, leftRewritten, rightProject, node.getCriteria(), Optional.<Symbol>absent(), Optional.<Symbol>absent());
//			return new JoinNode(idAllocator.getNextId(), JoinNode.Type.INNER, leftSemiJoin, rightSemiJoin, node.getCriteria(), Optional.<Symbol>absent(), Optional.<Symbol>absent()); 
//		}
		
		@Override
		public PlanNode rewriteTableScan(TableScanNode node, Void context, com.facebook.presto.sql.planner.plan.PlanRewriter<Void> planRewriter) {
			
			String schemaName = metadata.getTableMetadata(node.getTable()).getTable().getSchemaName();
			String tableName = metadata.getTableMetadata(node.getTable()).getTable().getTableName();
			String catalogName = getCatalogName(metadata, node);
			
			Optional<SemijoinMetadata> semimeta = metadata.getSemijoinMetadata(session, new QualifiedTableName(catalogName, schemaName, tableName));
			if(semimeta.isPresent())
				semijoinMetadata.put(node.getId(), semimeta.get());
			return super.rewriteTableScan(node, context, planRewriter);
		};
		
	}
	
	private static String getFullTableName(TableScanNode node, Metadata metadata)
	{
		String schemaName = metadata.getTableMetadata(node.getTable()).getTable().getSchemaName();
		String tableName = metadata.getTableMetadata(node.getTable()).getTable().getTableName();
		String catalogName = getCatalogName(metadata, node);
		return catalogName + "." + schemaName + "." + tableName;
	}
	
	private static String getCatalogName(Metadata metadata, TableScanNode node){
		Map<String, String> catalogMap = metadata.getCatalogNames();
		String connectorId = node.getTable().getConnectorId();
		for(String catalogName : catalogMap.keySet())
		{
			if(catalogMap.get(catalogName).equals(connectorId))
				return catalogName;
		}
		return null;
	}
	
	private static boolean isJoinNode(PlanNode node){
		if(node instanceof JoinNode)
			return true;
		else if(node.getSources()!= null && node.getSources().size() == 0)
			return false;
		else
			return isJoinNode(node.getSources().get(0));
			
	}
	
	private static TableScanNode getSourceTable(PlanNode node){
		if(node instanceof TableScanNode)
			return (TableScanNode)node;
		while(node.getSources() != null && node.getSources().size() == 1)
			return getSourceTable(node.getSources().get(0));
		return null;
	}
	
	/**
	 * 只适用于非join节点
	 * @param node
	 * @param idAllocator
	 * @return
	 */
	private static PlanNode copyNode(PlanNode node, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator){
		PlanNode source = null;
		if(node.getSources() != null && node.getSources().size() == 1)
		{
			source = copyNode(node.getSources().get(0), idAllocator, symbolAllocator);
		}
		if(node instanceof TableScanNode)
			return copyTableScanNode((TableScanNode)node, idAllocator, symbolAllocator);
		else if(node instanceof ProjectNode)
			return copyProjectNode((ProjectNode)node, source, idAllocator, symbolAllocator);
		else if(node instanceof FilterNode)
			return copyFilterNode((FilterNode)node, source, idAllocator);
		else
			return null;
	}
	
	private static TableScanNode copyTableScanNode(TableScanNode node, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator){
		List<Symbol> outputSymbol = new ArrayList<Symbol>();
		Map<Symbol, ColumnHandle> assignment = new HashMap<Symbol, ColumnHandle>();
		for(Symbol symbol : node.getOutputSymbols())
		{
			Symbol newSymbol = symbolAllocator.newSymbol(getSymbolKey(symbol), symbolAllocator.getTypes().get(symbol));
			outputSymbol.add(newSymbol);
			assignment.put(newSymbol, node.getAssignments().get(symbol));
		}
		return new TableScanNode(idAllocator.getNextId(), node.getTable(), outputSymbol, assignment, node.getOriginalConstraint());
	}
	
	private static ProjectNode copyProjectNode(ProjectNode node, PlanNode newSource, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator){
		Map<Symbol, Expression> assignment = new HashMap<Symbol, Expression>();
		for(Symbol symbol : node.getAssignments().keySet())
		{
			Symbol newSymbol = symbolAllocator.newSymbol(getSymbolKey(symbol), symbolAllocator.getTypes().get(symbol));
			Expression expression = null;
			for(Symbol sourceSymbol : newSource.getOutputSymbols())
			{
				if(getSymbolKey(sourceSymbol).equals(getSymbolKey(newSymbol)))
				{
					expression = new QualifiedNameReference(sourceSymbol.toQualifiedName());
					break;
				}
			}
			if(expression != null)
				assignment.put(newSymbol, expression);
		}
		return new ProjectNode(idAllocator.getNextId(), newSource, assignment);
	}
	
	private static FilterNode copyFilterNode(FilterNode node, PlanNode newSource, PlanNodeIdAllocator idAllocator){
		Expression newExpression = copyExpression(node.getPredicate(), newSource.getOutputSymbols());
		return new FilterNode(idAllocator.getNextId(), newSource, newExpression);
	}
	
	private static Expression copyExpression(Expression expression, List<Symbol> newSymbols){
		if(expression instanceof ComparisonExpression)
			return copyComparisonExpression((ComparisonExpression)expression, newSymbols);
		else if(expression instanceof LogicalBinaryExpression)
			return copyLogicalBinaryExpression((LogicalBinaryExpression)expression, newSymbols);
		else
			return null;
	}
	
	private static Expression copyLogicalBinaryExpression(LogicalBinaryExpression expression, List<Symbol> newSymbols){
		Expression left = copyExpression(expression.getLeft(), newSymbols);
		Expression right = copyExpression(expression.getRight(), newSymbols);
		return new LogicalBinaryExpression(expression.getType(), left, right);
	}
	
	private static Expression copyComparisonExpression(ComparisonExpression expression, List<Symbol> newSymbols){
		for (Symbol symbol : newSymbols)
			if (getSymbolKey(symbol).equals(expression.getLeft()))
				return new ComparisonExpression(expression.getType(),
						new QualifiedNameReference(symbol.toQualifiedName()),
						expression.getRight());
		return null;
	}
	
	private static boolean isContain(List<EquiJoinClause> subClause, List<EquiJoinClause> clause){
		return true;
	}
	
	private static boolean isSymbolEqual(List<Symbol> a, List<Symbol> b){
		if(a.size() != b.size())
			return false;
		int size = a.size();
		for(int i = 0; i < size; i++)
		{
			if(!a.get(i).toQualifiedName().equals(b.get(i).toQualifiedName()))
				return false;
		}
		return true;
	}
	
	private static List<EquiJoinClause> getCriteria(List<Symbol> a, List<Symbol> b){
		int size = a.size();
		List<EquiJoinClause> criteria = new ArrayList<EquiJoinClause>();
		for(int i = 0; i < size; i++)
		{
			Symbol symbolA = a.get(i);
			for(int j = 0; j < size; j++)
			{
				Symbol symbolB = b.get(j);
				if(getSymbolKey(symbolA).equals(getSymbolKey(symbolB)))
				{
					criteria.add(new EquiJoinClause(symbolA, symbolB));
					break;
				}
			}
		}
		return criteria;
	}
	
	private static Symbol getEqualSymbol(Symbol symbol, List<Symbol> array){
		for(Symbol instance : array)
		{
			if(getSymbolKey(symbol).equals(getSymbolKey(instance)))
				return instance;
		}
		return null;
	}
	
	private static String getSymbolKey(Symbol symbol){
		String name = symbol.getName();
		int index = name.lastIndexOf("_");
		if(index < name.length() - 1 && Character.isDigit(name.charAt(index + 1)))
			return name.substring(0, index);
		else
			return name;
	}
	
	
	private static List<EquiJoinClause> getCriteriaFromEquiJoinClause(List<EquiJoinClause> origin, List<Symbol> outputs){
		List<EquiJoinClause> criteria = new ArrayList<EquiJoinClause>();
		for(EquiJoinClause clause : origin)
		{
			Symbol right = clause.getRight();
			Symbol newRight = getEqualSymbol(right, outputs);
			EquiJoinClause newClause = new EquiJoinClause(clause.getLeft(), newRight);
			criteria.add(newClause);
		}
		return criteria;
	}
	
	/**
	 * 
	 * @param node 根节点
	 * @param source 目标父节点
	 * @return
	 */
	private static PlanNode getSon(PlanNode node, PlanNode source){
		List<PlanNode> sources = node.getSources();
		for(PlanNode ins : sources)
		{
			if(ins.getId().equals(source.getId()))
				return node;
		}
		for(PlanNode ins : sources)
		{
			PlanNode son = getSon(ins, source);
			if(son != null)
				return son;
		}
		return null;
	}
	
	
	/**
	 * 选取重复的输出，并构建唯一输出
	 * @param node
	 * @return
	 */
	private static List<Symbol> getUniqueSymbols(PlanNode node){
		List<Symbol> symbols = node.getOutputSymbols();
		List<Symbol> projectSymbols = new ArrayList<Symbol>();
		for(Symbol symbol : symbols)
		{
			if(isContain(projectSymbols, symbol))
				continue;
			projectSymbols.add(symbol);
		}
		return projectSymbols;
	}
	
	private static Map<Symbol, Expression> getAssignments(List<Symbol> list, SymbolAllocator symbolAllocator){
		Map<Symbol, Expression> assignments = new HashMap<Symbol, Expression>();
		for(Symbol symbol : list)
		{
			Symbol newSymbol = symbolAllocator.newSymbol(getSymbolKey(symbol), symbolAllocator.getTypes().get(symbol));
			Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
			assignments.put(newSymbol, expression);
		}
		return assignments;
	}
	
	private static boolean isContain(List<JoinNode> list, PlanNode node){
		for(PlanNode ins : list)
			if(ins.getId().equals(node.getId()))
				return true;
		return false;
	}
	
	private static boolean isContain(List<Symbol> symbols, Symbol symbol){
		for(Symbol ins : symbols)
		{
			if(getSymbolKey(ins).equals(getSymbolKey(symbol)))
				return true;
		}
		return false;
	}
	
	
	//TODO 测试创建symbol实例
	public static void main(String[] args) {
		SymbolAllocator symbolAllocator = new SymbolAllocator();
		System.out.println(symbolAllocator.newSymbol("name", null));
		System.out.println(symbolAllocator.newSymbol("name", null));
		System.out.println(symbolAllocator.newSymbol("name", null));
		System.out.println(symbolAllocator.newSymbol("name", null));
//		Symbol symbol = new Symbol("asdf_9");
//		System.out.println(getSymbolKey(symbol));
	}
}
