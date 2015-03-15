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
package com.facebook.presto.sql.planner.optimizations.estimater;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;

public class CostEstimater {

	/**
     * 参考derby数据库实现
     */
    public static double selectivity(Expression expression) {
		if (expression instanceof ComparisonExpression) {
			ComparisonExpression ce = (ComparisonExpression) expression;
			if (ce.getType() == ComparisonExpression.Type.EQUAL)
				return 0.1;
			if (ce.getType() == ComparisonExpression.Type.GREATER_THAN
					|| ce.getType() == ComparisonExpression.Type.GREATER_THAN_OR_EQUAL)
				return 0.33;
			if (ce.getType() == ComparisonExpression.Type.LESS_THAN
					|| ce.getType() == ComparisonExpression.Type.LESS_THAN_OR_EQUAL)
				return 0.33;
			if (ce.getType() == ComparisonExpression.Type.NOT_EQUAL)
				return 0.9;
		} 
		else if (expression instanceof InPredicate) 
			return 0.2;
		 else if (expression instanceof LikePredicate)
			return 0.9;
		else if (expression instanceof IsNullPredicate)
			return 0.1;
		else if (expression instanceof IsNotNullPredicate)
			return 0.9;
		
		return 0.5;

    }
}
