/**
 * Copyright © 2018 spring-data-dynamodb (https://github.com/boostchicken/spring-data-dynamodb)
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
package org.socialsignin.spring.data.dynamodb.repository.query;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import org.socialsignin.spring.data.dynamodb.core.DynamoDBOperations;
import org.socialsignin.spring.data.dynamodb.query.Query;
import org.socialsignin.spring.data.dynamodb.repository.ExpressionAttribute;
import org.socialsignin.spring.data.dynamodb.repository.QueryConstants;
import org.socialsignin.spring.data.dynamodb.repository.support.DynamoDBEntityInformation;
import org.socialsignin.spring.data.dynamodb.repository.support.DynamoDBIdIsHashAndRangeKeyEntityInformation;
import org.socialsignin.spring.data.dynamodb.repository.support.ExpressionAttributeHolder;
import org.socialsignin.spring.data.dynamodb.repository.support.MappedExpressionHolder;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Michael Lavelle
 * @author Sebastian Just
 */
public abstract class AbstractDynamoDBQueryCreator<T, ID, R>
        extends
        AbstractQueryCreator<Query<R>, DynamoDBQueryCriteria<T, ID>> {

    private int keyIdx = 1;
    private int valueIdx = 1;

    protected final DynamoDBEntityInformation<T, ID> entityMetadata;
    protected final DynamoDBOperations dynamoDBOperations;
    protected final Optional<String> projection;
    protected final Optional<Integer> limit;
    protected Optional<String> filterExpression;
    protected List<ExpressionAttributeHolder> expressionAttributeNames;
    protected List<ExpressionAttributeHolder> expressionAttributeValues;
    protected Map<String, MappedExpressionHolder> mappedExpressionValues;
    protected final QueryConstants.ConsistentReadMode consistentReads;

    public AbstractDynamoDBQueryCreator(
            PartTree tree,
            DynamoDBEntityInformation<T, ID> entityMetadata,
            Optional<String> projection,
            Optional<Integer> limitResults,
            QueryConstants.ConsistentReadMode consistentReads,
            Optional<String> filterExpression,
            ExpressionAttribute[] names,
            ExpressionAttribute[] values,
            DynamoDBOperations dynamoDBOperations) {
        super(tree);
        this.entityMetadata = entityMetadata;
        this.projection = projection;
        this.limit = limitResults;
        this.consistentReads = consistentReads;
        this.filterExpression = filterExpression;
        this.expressionAttributeNames = toHolder(names);
        this.expressionAttributeValues = toHolder(values);
        this.dynamoDBOperations = dynamoDBOperations;
        this.mappedExpressionValues = new HashMap<>();
    }

    public AbstractDynamoDBQueryCreator(
            PartTree tree,
            ParameterAccessor parameterAccessor,
            DynamoDBEntityInformation<T, ID> entityMetadata,
            Optional<String> projection,
            Optional<Integer> limitResults,
            QueryConstants.ConsistentReadMode consistentReads
            , Optional<String> filterExpression,
            ExpressionAttribute[] names,
            ExpressionAttribute[] values,
            DynamoDBOperations dynamoDBOperations) {
        super(tree, parameterAccessor);
        this.entityMetadata = entityMetadata;
        this.projection = projection;
        this.limit = limitResults;
        this.filterExpression = filterExpression;
        this.consistentReads = consistentReads;
        this.expressionAttributeNames = toHolder(names);
        this.expressionAttributeValues = toHolder(values);
        this.mappedExpressionValues = populateMappedExpressionValues(expressionAttributeValues, parameterAccessor);
        this.dynamoDBOperations = dynamoDBOperations;

        if (!filterExpression.isPresent()) {
            this.expressionAttributeValues = new ArrayList<>();
            this.expressionAttributeNames = new ArrayList<>();
            final StringBuilder filter = new StringBuilder();
            final Iterator<Object> arguments = parameterAccessor.iterator();
            for (Iterator<PartTree.OrPart> iter = tree.iterator(); iter.hasNext(); ) {
                addOr(iter.next(), arguments, filter);
                if (iter.hasNext()) {
                    filter.append(" OR ");
                }
            }
            this.filterExpression = Optional.of(filter.toString());
        }
    }


    private void addOr(PartTree.OrPart part, Iterator<Object> arguments, StringBuilder filter) {
        for (Iterator<Part> iter = part.iterator(); iter.hasNext(); ) {
            addAnd(iter.next(), arguments, filter);
            if (iter.hasNext()) {
                filter.append(" AND ");
            }
        }
    }

    private void addAnd(Part part, Iterator<Object> arguments, StringBuilder filter) {
        //see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html

        switch (part.getType()) {
            case BETWEEN:
            case WITHIN:
                addKey(part, filter);
                filter.append(" BETWEEN ");
                addValue(arguments, part.getProperty().getSegment(), filter);
                filter.append(" AND ");
                addValue(arguments, part.getProperty().getSegment(), filter);
                break;
            case STARTING_WITH:
                filter.append("begins_with(");
                addKey(part, filter);
                addValue(arguments, part.getProperty().getSegment(), filter);
                filter.append(")");
                break;
            case EXISTS:
                filter.append("attribute_exists(");
                addKey(part, filter);
                filter.append(")");
                break;
            case IS_NULL:
                filter.append("attribute_not_exists(");
                addKey(part, filter);
                filter.append(")");
                break;
            case BEFORE:
            case LESS_THAN:
                addKey(part, filter);
                filter.append(" < ");
                addValue(arguments, part.getProperty().getSegment(), filter);
                break;
            case LESS_THAN_EQUAL:
                addKey(part, filter);
                filter.append(" <= ");
                addValue(arguments, part.getProperty().getSegment(), filter);
                break;
            case AFTER:
            case GREATER_THAN:
                addKey(part, filter);
                filter.append(" > ");
                addValue(arguments, part.getProperty().getSegment(), filter);
                break;
            case GREATER_THAN_EQUAL:
                addKey(part, filter);
                filter.append(" >= ");
                addValue(arguments, part.getProperty().getSegment(), filter);
                break;
            case NOT_CONTAINING:
                filter.append("NOT ");
                contains(part, arguments, filter);
                break;
            case CONTAINING:
                contains(part, arguments, filter);
                break;
            case NOT_IN:
                addKey(part, filter);
                filter.append(" NOT IN(");
                iterateValues(part, arguments, part.getProperty().getSegment(), filter);
                filter.append(")");
                break;
            case IN:
                addKey(part, filter);
                filter.append(" IN (");
                iterateValues(part, arguments, part.getProperty().getSegment(), filter);
                filter.append(")");
                break;
            case TRUE:
                addKey(part, filter);
                break;
            case FALSE:
                filter.append("NOT ");
                addKey(part, filter);
                break;
            case IS_NOT_NULL:
            case NEGATING_SIMPLE_PROPERTY:
                addKey(part, filter);
                filter.append(" <> ");
                addValue(arguments, part.getProperty().getSegment(), filter);
                break;
            case SIMPLE_PROPERTY:
                addKey(part, filter);
                filter.append(" = ");
                addValue(arguments, part.getProperty().getSegment(), filter);
                break;
            //TODO
            case IS_NOT_EMPTY:
            case IS_EMPTY:
                //This may be a SS, L, BS, S

                // unsupported
            case NEAR:

            case REGEX:

            case NOT_LIKE:
            case LIKE:

            case ENDING_WITH:
            default:
                throw new UnsupportedOperationException("Not supported");
        }
    }

    private Collection<Object> getElements(Part part, Iterator<Object> arguments) {
        final Collection<Object> elements = new ArrayList<>();
        if (part.getNumberOfArguments() == 1) {
            Object param = arguments.next();
            if (param instanceof Collection) {
                elements.addAll((Collection<?>) param);
            } else if (ObjectUtils.isArray(param)) {
                elements.addAll(Arrays.asList(ObjectUtils.toObjectArray(param)));
            } else {
                elements.add(param);
            }
        } else {
            for (int i = 0; i < part.getNumberOfArguments(); i++) {
                elements.add(arguments.next());
            }
        }
        return elements;
    }

    private void contains(Part part, Iterator<Object> arguments, StringBuilder filter) {
        Collection<Object> elements = getElements(part, arguments);

        Iterator<Object> iter = elements.iterator();
        while (iter.hasNext()) {
            filter.append("contains(");
            addKey(part, filter);
            filter.append(",");
            addValue(iter, part.getProperty().getSegment(), filter);
            filter.append(")");
            if (iter.hasNext()) {
                filter.append(" AND ");
            }
        }
    }

    private void iterateValues(Part part, Iterator<Object> arguments, String propertyName, StringBuilder filter) {
        Collection<Object> elements = getElements(part, arguments);
        Iterator<Object> iter = elements.iterator();
        while (iter.hasNext()) {
            addValue(iter, propertyName, filter);
            if (iter.hasNext()) {
                filter.append(",");
            }
        }
    }

    private void addKey(Part part, StringBuilder filter) {
        String keyKey = "#key" + keyIdx;
        filter.append(keyKey);
        expressionAttributeNames.add(new ExpressionAttributeHolder(keyKey, part.getProperty().getSegment()));
        keyIdx++;
    }

    private void addValue(Iterator<?> iter, String propertyName, StringBuilder filter) {
        String valueKey = ":value" + valueIdx;
        filter.append(valueKey);
        expressionAttributeValues.add(new ExpressionAttributeHolder(valueKey, valueKey));
        mappedExpressionValues.put(valueKey, new MappedExpressionHolder(iter.next(), propertyName));
        valueIdx++;
    }

    protected Map<String, MappedExpressionHolder> populateMappedExpressionValues(List<ExpressionAttributeHolder> expressionAttributeValues, ParameterAccessor parameterAccessor) {
        if (expressionAttributeValues == null) {
            return new HashMap<>();
        }
        Map<String, MappedExpressionHolder> result = new HashMap<>();
        expressionAttributeValues
                .stream()
                .filter(value -> !StringUtils.isEmpty(value.getValue()))
                .forEach(value -> {
                    for (Parameter p : ((ParametersParameterAccessor) parameterAccessor).getParameters()) {
                        if (p.getName().isPresent() && p.getName().get().equals(value.getValue())) {
                            result.put(value.getValue(),
                                    new MappedExpressionHolder(
                                            parameterAccessor.getBindableValue(p.getIndex()), null));
                        }
                    }
                });
        return result;
    }


    private List<ExpressionAttributeHolder> toHolder(ExpressionAttribute[] attributes) {
        if (attributes == null) {
            return null;
        }
        List<ExpressionAttributeHolder> result = new ArrayList<>();
        for (ExpressionAttribute attribute : attributes) {
            String value = attribute.parameterName();
            if (StringUtils.isEmpty(value)) {
                value = attribute.value();
            }
            result.add(new ExpressionAttributeHolder(
                    attribute.key(),
                    value));
        }
        return result;
    }

    @Override
    protected DynamoDBQueryCriteria<T, ID> create(Part part, Iterator<Object> iterator) {
        final DynamoDBMapperTableModel<T> tableModel = dynamoDBOperations.getTableModel(entityMetadata.getJavaType());
        DynamoDBQueryCriteria<T, ID> criteria = entityMetadata.isRangeKeyAware()
                ? new DynamoDBEntityWithHashAndRangeKeyCriteria<>(
                (DynamoDBIdIsHashAndRangeKeyEntityInformation<T, ID>) entityMetadata, tableModel)
                : new DynamoDBEntityWithHashKeyOnlyCriteria<>(entityMetadata, tableModel);
        return addCriteria(criteria, part, iterator);
    }

    protected DynamoDBQueryCriteria<T, ID> addCriteria(DynamoDBQueryCriteria<T, ID> criteria, Part part,
                                                       Iterator<Object> iterator) {
        if (part.shouldIgnoreCase().equals(IgnoreCaseType.ALWAYS))
            throw new UnsupportedOperationException("Case insensitivity not supported");

        Class<?> leafNodePropertyType = part.getProperty().getLeafProperty().getType();

        PropertyPath leafNodePropertyPath = part.getProperty().getLeafProperty();
        String leafNodePropertyName = leafNodePropertyPath.toDotPath();
        if (leafNodePropertyName.contains(".")) {
            int index = leafNodePropertyName.lastIndexOf(".");
            leafNodePropertyName = leafNodePropertyName.substring(index);
        }

        switch (part.getType()) {
            case IN:
                return getInProperty(criteria, iterator, leafNodePropertyType, leafNodePropertyName);
            case CONTAINING:
                return getItemsProperty(criteria, ComparisonOperator.CONTAINS, iterator, leafNodePropertyType, leafNodePropertyName);
            case NOT_CONTAINING:
                return getItemsProperty(criteria, ComparisonOperator.NOT_CONTAINS, iterator, leafNodePropertyType, leafNodePropertyName);
            case STARTING_WITH:
                return criteria.withSingleValueCriteria(leafNodePropertyName, ComparisonOperator.BEGINS_WITH,
                        iterator.next(), leafNodePropertyType);
            case BETWEEN:
                Object first = iterator.next();
                Object second = iterator.next();
                return criteria.withPropertyBetween(leafNodePropertyName, first, second, leafNodePropertyType);
            case AFTER:
            case GREATER_THAN:
                return criteria.withSingleValueCriteria(leafNodePropertyName, ComparisonOperator.GT, iterator.next(),
                        leafNodePropertyType);
            case BEFORE:
            case LESS_THAN:
                return criteria.withSingleValueCriteria(leafNodePropertyName, ComparisonOperator.LT, iterator.next(),
                        leafNodePropertyType);
            case GREATER_THAN_EQUAL:
                return criteria.withSingleValueCriteria(leafNodePropertyName, ComparisonOperator.GE, iterator.next(),
                        leafNodePropertyType);
            case LESS_THAN_EQUAL:
                return criteria.withSingleValueCriteria(leafNodePropertyName, ComparisonOperator.LE, iterator.next(),
                        leafNodePropertyType);
            case IS_NULL:
                return criteria.withNoValuedCriteria(leafNodePropertyName, ComparisonOperator.NULL);
            case IS_NOT_NULL:
                return criteria.withNoValuedCriteria(leafNodePropertyName, ComparisonOperator.NOT_NULL);
            case TRUE:
                return criteria.withSingleValueCriteria(leafNodePropertyName, ComparisonOperator.EQ, Boolean.TRUE,
                        leafNodePropertyType);
            case FALSE:
                return criteria.withSingleValueCriteria(leafNodePropertyName, ComparisonOperator.EQ, Boolean.FALSE,
                        leafNodePropertyType);
            case SIMPLE_PROPERTY:
                return criteria.withPropertyEquals(leafNodePropertyName, iterator.next(), leafNodePropertyType);
            case NEGATING_SIMPLE_PROPERTY:
                return criteria.withSingleValueCriteria(leafNodePropertyName, ComparisonOperator.NE, iterator.next(),
                        leafNodePropertyType);
            default:
                throw new IllegalArgumentException("Unsupported keyword " + part.getType());
        }

	}

    private DynamoDBQueryCriteria<T, ID> getItemsProperty(DynamoDBQueryCriteria<T, ID> criteria, ComparisonOperator comparisonOperator, Iterator<Object> iterator, Class<?> leafNodePropertyType, String leafNodePropertyName) {
        Object in = iterator.next();
        Assert.notNull(in, "Creating conditions on null parameters not supported: please specify a value for '" + leafNodePropertyName + "'");

        if(ObjectUtils.isArray(in)) {
            List<?> list = Arrays.asList(ObjectUtils.toObjectArray(in));
            Object value = list.get(0);
            return criteria.withSingleValueCriteria(leafNodePropertyName, comparisonOperator, value, leafNodePropertyType);
        } else if(ClassUtils.isAssignable(Iterable.class, in.getClass())) {
            Iterator<?> iter = ((Iterable<?>) in).iterator();
            Assert.isTrue(iter.hasNext(), "Creating conditions on empty parameters not supported: please specify a value for '\" + leafNodePropertyName + \"'\"");
            Object value = iter.next();
            return criteria.withSingleValueCriteria(leafNodePropertyName, comparisonOperator, value, leafNodePropertyType);
        } else {
            return criteria.withSingleValueCriteria(leafNodePropertyName, comparisonOperator, in, leafNodePropertyType);
        }
    }

    private DynamoDBQueryCriteria<T, ID> getInProperty(DynamoDBQueryCriteria<T, ID> criteria, Iterator<Object> iterator, Class<?> leafNodePropertyType, String leafNodePropertyName) {
        Object in = iterator.next();
        Assert.notNull(in, "Creating conditions on null parameters not supported: please specify a value for '"
                + leafNodePropertyName + "'");
        boolean isIterable = ClassUtils.isAssignable(Iterable.class, in.getClass());
        boolean isArray = ObjectUtils.isArray(in);
        Assert.isTrue(isIterable || isArray, "In criteria can only operate with Iterable or Array parameters");
        Iterable<?> iterable = isIterable ? ((Iterable<?>) in) : Arrays.asList(ObjectUtils.toObjectArray(in));
        return criteria.withPropertyIn(leafNodePropertyName, iterable, leafNodePropertyType);
    }

    @Override
	protected DynamoDBQueryCriteria<T, ID> and(Part part, DynamoDBQueryCriteria<T, ID> base,
			Iterator<Object> iterator) {
		return addCriteria(base, part, iterator);

	}

	@Override
	protected DynamoDBQueryCriteria<T, ID> or(DynamoDBQueryCriteria<T, ID> base,
			DynamoDBQueryCriteria<T, ID> criteria) {
        return base;
    }

}
