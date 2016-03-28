/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.mapping;

import java.util.Comparator;

/**
 * {@link Comparator} implementation that orders {@link CassandraPersistentProperty} instances.
 * <p/>
 * Composite primary key properties and primary key properties sort before non-primary key properties.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public enum CassandraPersistentPropertyComparator implements Comparator<CassandraPersistentProperty> {

	/**
	 * The sole instance of this class.
	 */
	IT;

	@Override
	public int compare(CassandraPersistentProperty left, CassandraPersistentProperty right) {

		if (left != null && right == null) {
			return 1;
		}
		if (left == null && right != null) {
			return -1;
		}
		if (left == null && right == null) {
			return 0;
		}

		if (left.equals(right)) {
			return 0;
		}
		
		// order id properties first
		
		boolean leftIsPkProp = left.isIdProperty() || left.isCompositePrimaryKey();
        boolean rightIsPkProp = right.isIdProperty() || right.isCompositePrimaryKey();

        int order;
        if ((order = orderBool(leftIsPkProp, rightIsPkProp)) == 0) {
    
            // it might be composite key entity
            
    		boolean leftIsPkCol = left.isPrimaryKeyColumn();
    		boolean rightIsPkCol = right.isPrimaryKeyColumn();
    
    		if ((order = orderBool(leftIsPkCol, rightIsPkCol)) == 0) {
    		    
    	        if (leftIsPkCol && rightIsPkCol) {
                    return CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(left.findAnnotation(PrimaryKeyColumn.class),
                            right.findAnnotation(PrimaryKeyColumn.class));
                }
        
        		// else, neither property is a composite primary key nor a primary key; compare @Column annotations
        
        		return left.getName().compareTo(right.getName());
    		}
        }
		return order;
	}

    private int orderBool(boolean leftIsPkProp, boolean rightIsPkProp) {
        if (leftIsPkProp && !rightIsPkProp) {
            return -1;
        } else if (!leftIsPkProp && rightIsPkProp) {
            return 1;
        } else {
            return 0;
        }
    }
}
