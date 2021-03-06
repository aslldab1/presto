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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.NamedParameterDefinition;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.gen.ByteCodeUtils;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.UnknownType;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.relational.Signatures.arrayConstructorSignature;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.invoke.MethodHandles.lookup;

public final class ArrayConstructor
        extends ParametricScalar
{
    public static final ArrayConstructor ARRAY_CONSTRUCTOR = new ArrayConstructor();
    private static final Signature SIGNATURE = new Signature("array_constructor", ImmutableList.of(typeParameter("E")), "array<E>", ImmutableList.of("E"), true, true);
    private static final MethodHandle EMPTY_ARRAY_CONSTRUCTOR = methodHandle(ArrayConstructor.class, "emptyArrayConstructor");

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean isHidden()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        // Internal function, doesn't need a description
        return "";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager)
    {
        // Check to see if we're creating an empty, un-specialized array
        if (types.isEmpty()) {
            return new FunctionInfo(arrayConstructorSignature(parameterizedTypeName("array", parseTypeSignature(UnknownType.NAME)), ImmutableList.<TypeSignature>of()), "", true, EMPTY_ARRAY_CONSTRUCTOR, true, false, ImmutableList.<Boolean>of());
        }

        checkArgument(types.size() == 1, "Can only construct arrays from exactly matching types");
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        Type type = types.get("E");
        for (int i = 0; i < arity; i++) {
            if (type.getJavaType().isPrimitive()) {
                builder.add(Primitives.wrap(type.getJavaType()));
            }
            else {
                builder.add(type.getJavaType());
            }
        }
        ImmutableList<Class<?>> stackTypes = builder.build();
        Class<?> clazz = generateArrayConstructor(stackTypes, type);
        MethodHandle methodHandle;
        try {
            Method method = clazz.getMethod("arrayConstructor", stackTypes.toArray(new Class<?>[stackTypes.size()]));
            methodHandle = lookup().unreflect(method);
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
        List<Boolean> nullableParameters = ImmutableList.copyOf(Collections.nCopies(stackTypes.size(), true));
        return new FunctionInfo(arrayConstructorSignature(parameterizedTypeName("array", type.getTypeSignature()), Collections.nCopies(arity, type.getTypeSignature())), "Constructs an array of the given elements", true, methodHandle, true, false, nullableParameters);
    }

    public static Slice emptyArrayConstructor()
    {
        return ArrayType.toStackRepresentation(ImmutableList.of());
    }

    private static Class<?> generateArrayConstructor(List<Class<?>> stackTypes, Type elementType)
    {
        List<String> stackTypeNames = FluentIterable.from(stackTypes).transform(new Function<Class<?>, String>() {
            @Override
            public String apply(Class<?> input)
            {
                return input.getSimpleName();
            }
        }).toList();
        ClassDefinition definition = new ClassDefinition(
                new CompilerContext(null),
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(Joiner.on("").join(stackTypeNames) + "ArrayConstructor"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Generate arrayConstructor()
        ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
        for (int i = 0; i < stackTypes.size(); i++) {
            Class<?> stackType = stackTypes.get(i);
            parameters.add(arg("arg" + i, stackType));
        }

        CompilerContext context = new CompilerContext(null);
        Block body = definition.declareMethod(context, a(PUBLIC, STATIC), "arrayConstructor", type(Slice.class), parameters.build())
                .getBody();

        Variable valuesVariable = context.declareVariable(List.class, "values");
        body.comment("List<Object> values = new ArrayList();")
                .newObject(ArrayList.class)
                .dup()
                .invokeConstructor(ArrayList.class)
                .putVariable(valuesVariable);

        for (int i = 0; i < stackTypes.size(); i++) {
            body.comment("values.add(arg" + i + ");")
                    .getVariable(valuesVariable)
                    .getVariable("arg" + i);
            Class<?> stackType = stackTypes.get(i);
            if (stackType.isPrimitive()) {
                body.append(ByteCodeUtils.boxPrimitiveIfNecessary(context, stackType));
            }
            body.invokeInterface(List.class, "add", boolean.class, Object.class);
        }

        if (elementType instanceof ArrayType || elementType instanceof MapType || elementType instanceof RowType) {
            body.comment("return rawSlicesToStackRepresentation(values);")
                    .getVariable(valuesVariable)
                    .invokeStatic(ArrayType.class, "rawSlicesToStackRepresentation", Slice.class, List.class)
                    .retObject();
        }
        else {
            body.comment("return toStackRepresentation(values);")
                    .getVariable(valuesVariable)
                    .invokeStatic(ArrayType.class, "toStackRepresentation", Slice.class, List.class)
                    .retObject();
        }

        return defineClass(definition, Object.class, new DynamicClassLoader());
    }
}
