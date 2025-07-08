/*
 * Copyright 2022 The Blaze Authors
 *
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
package org.apache.flink.table.planner.plan.nodes.exec.common;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.utils.FunctionResultTermCache;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.blaze.flink.table.runtime.operator.Arrow2RowDataOperator;
import org.blaze.flink.table.runtime.operator.FlinkBlazeCalcOperator;

/** Base class for exec Calc. */
public abstract class CommonExecCalc extends ExecNodeBase<RowData> implements SingleTransformationTranslator<RowData> {

    public static final String CALC_TRANSFORMATION = "calc";

    public static final String FIELD_NAME_PROJECTION = "projection";
    public static final String FIELD_NAME_CONDITION = "condition";

    @JsonProperty(FIELD_NAME_PROJECTION)
    protected final List<RexNode> projection;

    @JsonProperty(FIELD_NAME_CONDITION)
    protected final @Nullable RexNode condition;

    private final Class<?> operatorBaseClass;
    private final boolean retainHeader;

    protected CommonExecCalc(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            Class<?> operatorBaseClass,
            boolean retainHeader,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.projection = checkNotNull(projection);
        this.condition = condition;
        this.operatorBaseClass = checkNotNull(operatorBaseClass);
        this.retainHeader = retainHeader;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform = (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final CodeGeneratorContext ctx = new CodeGeneratorContext(
                        config, planner.getFlinkContext().getClassLoader())
                .setOperatorBaseClass(operatorBaseClass);

        //        final CodeGenOperatorFactory<RowData> substituteStreamOperator =
        //                CalcCodeGenerator.generateCalcOperator(
        //                        ctx,
        //                        inputTransform,
        //                        (RowType) getOutputType(),
        //                        planner.getTableConfig(),
        //                        JavaScalaConversionUtil.toScala(projection),
        //                        JavaScalaConversionUtil.toScala(Optional.ofNullable(this.condition)),
        //                        retainHeader,
        //                        getClass().getSimpleName());
        //        Transformation<RowData> ret =
        //                ExecNodeUtil.createOneInputTransformation(
        //                        inputTransform,
        //                        createTransformationMeta(CALC_TRANSFORMATION, config),
        //                        substituteStreamOperator,
        //                        InternalTypeInfo.of(getOutputType()),
        //                        inputTransform.getParallelism(),
        //                        false);

        RowType inputType = ((InternalTypeInfo) inputTransform.getOutputType()).toRowType();

        Transformation<RowData> blazeCalc = ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta("blaze_calc", config),
                new FlinkBlazeCalcOperator(inputType),
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism(),
                false);

        Transformation<RowData> arrow2RowData = ExecNodeUtil.createOneInputTransformation(
                blazeCalc,
                createTransformationMeta("arrow2RowData", config),
                new Arrow2RowDataOperator(),
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism(),
                false);
        FunctionResultTermCache.clearCache();
        System.out.println("CommonExecCalc222");
        return arrow2RowData;
    }
}
