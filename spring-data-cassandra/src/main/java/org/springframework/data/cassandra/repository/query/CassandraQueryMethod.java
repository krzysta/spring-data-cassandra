package org.springframework.data.cassandra.repository.query;

import com.datastax.driver.core.*;
import com.datastax.driver.core.DataType.Name;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.cql.CqlStringUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraType;
import org.springframework.data.cassandra.mapping.TableDiscriminator;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

public class CassandraQueryMethod extends QueryMethod {

	// TODO: double-check this list
	public static final List<Class<?>> ALLOWED_PARAMETER_TYPES = Collections.unmodifiableList(Arrays
			.asList(new Class<?>[] { String.class, CharSequence.class, char.class, Character.class, char[].class, long.class,
					Long.class, boolean.class, Boolean.class, BigDecimal.class, BigInteger.class, double.class, Double.class,
					float.class, Float.class, InetAddress.class, Date.class, UUID.class, int.class, Integer.class,
					List.class, Set.class, Array.class }));

	public static final List<Class<?>> STRING_LIKE_PARAMETER_TYPES = Collections.unmodifiableList(Arrays
			.asList(new Class<?>[] { CharSequence.class, char.class, Character.class, char[].class }));

	public static final List<Class<?>> DATE_PARAMETER_TYPES = Collections.unmodifiableList(Arrays
			.asList(new Class<?>[] { Date.class }));

	public static final List<Class<?>> COLLECTION_LIKE_PARAMETER_TYPES = Collections.unmodifiableList(Arrays
			.asList(new Class<?>[] { List.class, Set.class, Array.class }));

	private static Map<Name, Class<?>> dataTypeToClassMap = new HashMap<Name, Class<?>>();

	static {
		dataTypeToClassMap.put(Name.ASCII, String.class);
		dataTypeToClassMap.put(Name.BIGINT, Long.class);
		dataTypeToClassMap.put(Name.BLOB, ByteBuffer.class);
		dataTypeToClassMap.put(Name.BOOLEAN, Boolean.class);
		dataTypeToClassMap.put(Name.COUNTER, Long.class);
		dataTypeToClassMap.put(Name.DECIMAL, BigDecimal.class);
		dataTypeToClassMap.put(Name.DOUBLE, Double.class);
		dataTypeToClassMap.put(Name.FLOAT, Float.class);
		dataTypeToClassMap.put(Name.INET, InetAddress.class);
		dataTypeToClassMap.put(Name.INT, Integer.class);
		dataTypeToClassMap.put(Name.TEXT, String.class);
		dataTypeToClassMap.put(Name.TIMESTAMP, Date.class);
		dataTypeToClassMap.put(Name.UUID, UUID.class);
		dataTypeToClassMap.put(Name.VARCHAR, String.class);
		dataTypeToClassMap.put(Name.VARINT, BigInteger.class);
		dataTypeToClassMap.put(Name.TIMEUUID, UUID.class);
		dataTypeToClassMap.put(Name.LIST, List.class);
		dataTypeToClassMap.put(Name.SET, Set.class);
		dataTypeToClassMap.put(Name.MAP, Map.class);
		dataTypeToClassMap.put(Name.UDT, UDTValue.class);
		dataTypeToClassMap.put(Name.TUPLE, TupleValue.class);
		dataTypeToClassMap.put(Name.CUSTOM, ByteBuffer.class);
		dataTypeToClassMap.put(Name.DATE, LocalDate.class);
		dataTypeToClassMap.put(Name.TIME, Long.class);
		dataTypeToClassMap.put(Name.SMALLINT, Short.class);
		dataTypeToClassMap.put(Name.TINYINT, Byte.class);
	}

	public static boolean isMapOfCharSequenceToObject(TypeInformation<?> type) {

		if (!type.isMap()) {
			return false;
		}

		TypeInformation<?> keyType = type.getComponentType();
		TypeInformation<?> valueType = type.getMapValueType();

		return ClassUtils.isAssignable(CharSequence.class, keyType.getType()) && Object.class.equals(valueType.getType());
	}

	protected Method method;
	protected CassandraMappingContext mappingContext;
	protected Query query;
	protected String queryString;
	protected boolean queryCached = false;
	protected Set<Integer> stringLikeParameterIndexes = new HashSet<Integer>();
	protected Set<Integer> dateParameterIndexes = new HashSet<Integer>();
	protected Integer discriminatorParameterIndex ;
	protected Map<Integer, Class<?>> collectionLikeParameterIndexes = new HashMap<Integer, Class<?>>();

	public CassandraQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory factory, CassandraMappingContext mappingContext) {

		super(method, metadata, factory);

		verify(method, metadata);

		this.method = method;

		Assert.notNull(mappingContext, "MappingContext must not be null!");
		this.mappingContext = mappingContext;
	}

	public void verify(Method method, RepositoryMetadata metadata) {

		// TODO: support Page & Slice queries
		if (isSliceQuery() || isPageQuery()) {
			throw new InvalidDataAccessApiUsageException("neither slice nor page queries are supported yet");
		}

		Set<Class<?>> offendingTypes = new HashSet<Class<?>>();

		int i = 0;
		Annotation[][] parameterAnnotations = method.getParameterAnnotations();
		for (Class<?> type : method.getParameterTypes()) {
			TableDiscriminator discriminator = findAnnotation(parameterAnnotations[i], TableDiscriminator.class);
			if (discriminator!=null){
				discriminatorParameterIndex = i;
			}

			CassandraType cnvAnn = findAnnotation(parameterAnnotations[i], CassandraType.class);
			if (cnvAnn == null) {
				cnvAnn = type.getAnnotation(CassandraType.class);
			}
			if (cnvAnn != null) {
				type = dataTypeToClassMap.get(cnvAnn.type());
			}

			// TODO: how about subtypes?
			if (!ALLOWED_PARAMETER_TYPES.contains(type)) {
				offendingTypes.add(type);
			}


			if (isStringLikeParameter(type)) {
				stringLikeParameterIndexes.add(i);
			}

			if (isDateParameter(type)) {
				dateParameterIndexes.add(i);
			}

			if (isCollectionLikeParameter(type)) {
				/*
				 * let's assume we require typeArguments to be present in annotation...
				 * later on we could handle it the same way this class handles type.
				 * typeArguments has length=1 for lists and length=2 for maps... we only handle lists here
				 */
				if (cnvAnn.typeArguments().length != 1) {
					throw new IllegalArgumentException(String.format(
							"encountered unsupported query parameter type%s [%s] in method %s: typeArguments property missing.",
							offendingTypes.size() == 1 ? "" : "s",
							StringUtils.arrayToCommaDelimitedString(new ArrayList<Class<?>>(offendingTypes).toArray()),
							method));
				}

				Class<?> subtype = dataTypeToClassMap.get(cnvAnn.typeArguments()[0]);
				collectionLikeParameterIndexes.put(i, subtype);
			}
			i++;
		}

		if (offendingTypes.size() > 0) {
			throw new IllegalArgumentException(String.format(
					"encountered unsupported query parameter type%s [%s] in method %s", offendingTypes.size() == 1 ? "" : "s",
					StringUtils.arrayToCommaDelimitedString(new ArrayList<Class<?>>(offendingTypes).toArray()), method));
		}
	}

    @SuppressWarnings("unchecked")
    private <T> T findAnnotation(Annotation[] annotations, Class<T> annType) {
        for (Annotation a : annotations) {
            if (annType.isAssignableFrom(a.annotationType())) {
                return (T) a;
            }
        }
        return null;
    }

    @Override
	protected CassandraParameters createParameters(Method method) {
		return new CassandraParameters(method);
	}

	/**
	 * Returns the {@link Query} annotation that is applied to the method or {@code null} if none available.
	 */
	Query getQueryAnnotation() {
		if (query == null) {
			query = method.getAnnotation(Query.class);
			queryCached = true;
		}
		return query;
	}

	/**
	 * Returns whether the method has an annotated query.
	 */
	public boolean hasAnnotatedQuery() {
		return getAnnotatedQuery() != null;
	}

	/**
	 * Returns the query string declared in a {@link Query} annotation or {@literal null} if neither the annotation found
	 * nor the attribute was specified.
	 */
	public String getAnnotatedQuery() {

		if (!queryCached) {
			queryString = (String) AnnotationUtils.getValue(getQueryAnnotation());
			queryString = StringUtils.hasText(queryString) ? queryString : null;
		}

		return queryString;
	}

	public TypeInformation<?> getReturnType() {
		return ClassTypeInformation.fromReturnTypeOf(method);
	}

	public boolean isResultSetQuery() {
		return ResultSet.class.isAssignableFrom(method.getReturnType());
	}

	public boolean isSingleEntityQuery() {
		return ClassUtils.isAssignable(getDomainClass(), method.getReturnType());
	}

	public boolean isCollectionOfEntityQuery() {
		return isQueryForEntity() && isCollectionQuery();
	}

	public boolean isVoidQuery(){
		return Void.TYPE.isAssignableFrom(method.getReturnType());
	}

	public boolean isMapOfCharSequenceToObjectQuery() {

		return isMapOfCharSequenceToObject(getReturnType());
	}

	public boolean isListOfMapOfCharSequenceToObject() {

		TypeInformation<?> type = getReturnType();
		if (!ClassUtils.isAssignable(List.class, type.getType())) {
			return false;
		}

		return isMapOfCharSequenceToObject(type.getComponentType());
	}

	public boolean isStringLikeParameter(int parameterIndex) {
		return stringLikeParameterIndexes.contains(parameterIndex);
	}

	public boolean isDateParameter(int parameterIndex) {
		return dateParameterIndexes.contains(parameterIndex);
	}

	public boolean isCollectionParameter(int parameterIndex) {
		return collectionLikeParameterIndexes.keySet().contains(parameterIndex);
	}

    public String convertParameterToCQL(int index, Object value, CassandraConverter converter) {
        if (isStringLikeParameter(index)) {
                String string = converter.getConversionService().convert(value, String.class);
                return "'" + CqlStringUtils.escapeSingle(string) + "'";
        } else if (isDateParameter(index)) {
            Date date = converter.getConversionService().convert(value, Date.class);
            return "" + date.getTime();
        } else if (isCollectionParameter(index)) {
            Collection<?> collection = Collection.class.cast(value instanceof Array ? Arrays.asList((Object[]) value) : value);

            String[] vals = new String[collection.size()];

            int i = 0;
            Class<?> argType = collectionLikeParameterIndexes.get(index);

            for (Object o : collection) {
                if (isStringLikeParameter(argType)) {
                    String string = converter.getConversionService().convert(o, String.class);
                    vals[i] = "'" + CqlStringUtils.escapeSingle(string) + "'";
                } else if (isDateParameter(argType)) {
                    Date date = converter.getConversionService().convert(o, Date.class);
                    vals[i] = "" + date.getTime();
                } else {
                    vals[i] = o.toString();
                }
                i++;
            }
            return StringUtils.arrayToCommaDelimitedString(vals);
        } else {
                return value.toString();
        }
    }

    private boolean isDateParameter(Class<?> type) {
        return containsType(DATE_PARAMETER_TYPES, type);
    }

    private boolean isStringLikeParameter(Class<?> type) {
        return containsType(STRING_LIKE_PARAMETER_TYPES, type);
    }

    private boolean isCollectionLikeParameter(Class<?> type) {
        return containsType(COLLECTION_LIKE_PARAMETER_TYPES, type);
    }

    private boolean containsType(List<Class<?>> typesCollection, Class<?> type) {
        for (Class<?> quotedType : typesCollection) {
            if (quotedType.isAssignableFrom(type)) {
                return true;
            }
        }
        return false;
    }

	public String resolveTableName(CassandraParameterAccessor accessor){
		Object bindableValue = null;
		if (discriminatorParameterIndex != null){
			bindableValue = accessor.getBindableValue(discriminatorParameterIndex);
		}
		CassandraPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(getDomainClass());
		CqlIdentifier tableName = persistentEntity.getEntityDiscriminator().getTableNameForDiscriminator(bindableValue);
		return tableName.toCql();
	}
}
