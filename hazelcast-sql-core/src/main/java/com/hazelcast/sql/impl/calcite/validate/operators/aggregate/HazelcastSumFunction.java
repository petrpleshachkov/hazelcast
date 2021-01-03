package com.hazelcast.sql.impl.calcite.validate.operators.aggregate;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastAbsFunction;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

// TODO temporary impl
public class HazelcastSumFunction extends HazelcastFunction {

    public static final HazelcastSumFunction SUM = new HazelcastSumFunction(SqlStdOperatorTable.SUM);

    private HazelcastSumFunction(SqlAggFunction base) {
        super(
            base.getName(),
            base.getKind(),
            base.getReturnTypeInference(),
            base.getOperandTypeInference(),
            base.getFunctionType()
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        return true;
    }
}
