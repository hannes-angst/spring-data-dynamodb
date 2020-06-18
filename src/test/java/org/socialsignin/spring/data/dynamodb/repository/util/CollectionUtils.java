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
package org.socialsignin.spring.data.dynamodb.repository.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class CollectionUtils {
    private CollectionUtils() {
        throw new RuntimeException("illegal instantiation");
    }


    public static <E> Set<E> setOf(E value) {
        Set<E> result = new HashSet<>();
        result.add(value);
        return result;
    }
    public static <E> Set<E> setOf(E value1, E value2) {
        Set<E> result = new HashSet<>();
        result.add(value1);
        result.add(value2);
        return result;
    }
    public static <E> Set<E> setOf(E value1, E value2, E value3) {
        Set<E> result = new HashSet<>();
        result.add(value1);
        result.add(value2);
        result.add(value3);
        return result;
    }
    public static <E> Set<E> setOf(E value1, E value2, E value3, E value4) {
        Set<E> result = new HashSet<>();
        result.add(value1);
        result.add(value2);
        result.add(value3);
        result.add(value4);
        return result;
    }
    @SafeVarargs
    public static <E> Set<E> setOf(E... values) {
        Set<E> result = new HashSet<>();

        if (values != null) {
            result.addAll(Arrays.asList(values));
        }
        return result;
    }


    public static <E> List<E> listOf(E value) {
        List<E> result = new ArrayList<>();
        result.add(value);
        return result;
    }
    public static <E> List<E> listOf(E value1, E value2) {
        List<E> result = new ArrayList<>();
        result.add(value1);
        result.add(value2);
        return result;
    }
    public static <E> List<E> listOf(E value1, E value2, E value3) {
        List<E> result = new ArrayList<>();
        result.add(value1);
        result.add(value2);
        result.add(value3);
        return result;
    }
    public static <E> List<E> listOf(E value1, E value2, E value3, E value4) {
        List<E> result = new ArrayList<>();
        result.add(value1);
        result.add(value2);
        result.add(value3);
        result.add(value4);
        return result;
    }

    @SafeVarargs
    public static <E> List<E> listOf(E... values) {
        List<E> result = new ArrayList<>();

        if (values != null) {
            result.addAll(Arrays.asList(values));
        }
        return result;
    }

    public static <K, V> Map<K, V> mapOf(K key, V value) {
        Map<K, V> result = new HashMap<>();
        result.put(key, value);
        return result;
    }

    public static <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2) {
        Map<K, V> result = new HashMap<>();
        result.put(key1, value1);
        result.put(key2, value2);
        return result;
    }

    public static <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2, K key3, V value3) {
        Map<K, V> result = new HashMap<>();
        result.put(key1, value1);
        result.put(key2, value2);
        result.put(key3, value3);
        return result;
    }

    public static <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4) {
        Map<K, V> result = new HashMap<>();
        result.put(key1, value1);
        result.put(key2, value2);
        result.put(key3, value3);
        return result;
    }
}
