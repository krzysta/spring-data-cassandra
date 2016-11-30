package org.springframework.data.cassandra.convert;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;

import java.util.*;

public class CassandraConvertingPropertyAccessor extends ConvertingPropertyAccessor {

    private ConversionService conversionService;

    public CassandraConvertingPropertyAccessor(PersistentPropertyAccessor accessor,
            ConversionService conversionService) {
        super(accessor, conversionService);
        this.conversionService = conversionService;
    }

    @SuppressWarnings("unchecked")
    private <T> T convert(Object source, Class<T> type) {
        return (T) (source == null ? source : type.isAssignableFrom(source.getClass()) ? source : conversionService
                .convert(source, type));
    }

    @SuppressWarnings("unchecked")
    private <T> T convert(Object source, DataType dt) {
        return (T) convert(source, CodecRegistry.DEFAULT_INSTANCE.codecFor(dt).getJavaType().getRawType());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getProperty(PersistentProperty<?> property, Class<T> targetType) {
        if (property instanceof CassandraPersistentProperty && !((CassandraPersistentProperty) property).isCompositePrimaryKey()) {
            Object value = super.getProperty(property);
            CassandraPersistentProperty cpp = (CassandraPersistentProperty) property;
            if (cpp.isCompositePrimaryKey()) {
                return (T) convert(value, cpp.getType());
            } else {
                DataType dt = cpp.getDataType();

                List<DataType> typeArgs = dt.getTypeArguments();
        
                if ((dt.getName() == Name.LIST || dt.getName() == Name.SET) && value instanceof Collection) {
                    return convert(convertCol((Collection) value,
                            CodecRegistry.DEFAULT_INSTANCE.codecFor(typeArgs.get(0)).getJavaType().getRawType()), dt);
                } else if ((dt.getName() == Name.MAP) && value instanceof Map) {
                    return convert(
                            convertMap((Map) value,
                                    CodecRegistry.DEFAULT_INSTANCE.codecFor(typeArgs.get(0)).getJavaType().getRawType(),
                                    CodecRegistry.DEFAULT_INSTANCE.codecFor(typeArgs.get(1)).getJavaType().getRawType()),
                            dt);
                } else {
                    return convert(value, dt);
                }
            }
        } else {
            return super.getProperty(property, targetType);
        }
    }

    private <V, TV> Collection convertCol(Collection<V> col, Class<TV> clazz) {
        List result = new ArrayList();
        for (Object obj : col) {
            result.add(convert(obj, clazz));
        }
        return result;
    }

    private <K, V, TK, TV> Map convertMap(Map<K, V> map, Class<TK> keyClazz, Class<TV> valClazz) {
        Map result = new LinkedHashMap();
        for (Map.Entry entry : map.entrySet()) {
            result.put(
                    convert(entry.getKey(), keyClazz),
                    convert(entry.getValue(), valClazz));
        }
        return result;
    }

    @Override
    public void setProperty(PersistentProperty<?> property, Object value) {
        if (property instanceof CassandraPersistentProperty  && !((CassandraPersistentProperty) property).isCompositePrimaryKey()) {

            CassandraPersistentProperty cpp = (CassandraPersistentProperty) property;
            if (cpp.isCompositePrimaryKey()) {
                super.setProperty(property, value);
            } else {
                DataType dt = cpp.getDataType();
    
                List<DataType> typeArgs = dt.getTypeArguments();
    
                if ((dt.getName() == Name.LIST || dt.getName() == Name.SET) && value instanceof Collection) {
                    super.setProperty(property, convertCol((Collection) value, cpp.getComponentType()));
                } else if ((dt.getName() == Name.MAP) && value instanceof Map) {
                    super.setProperty(
                            property,
                            convertMap((Map) value, cpp.getComponentType(), cpp.getMapValueType()));
                } else {
                    super.setProperty(property, value);
                }
            }
        } else {
            super.setProperty(property, value);
        }
    }

}
