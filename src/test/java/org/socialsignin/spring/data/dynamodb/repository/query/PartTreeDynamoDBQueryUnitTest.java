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

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.socialsignin.spring.data.dynamodb.core.DynamoDBOperations;
import org.socialsignin.spring.data.dynamodb.domain.sample.DynamoDBYearMarshaller;
import org.socialsignin.spring.data.dynamodb.domain.sample.Playlist;
import org.socialsignin.spring.data.dynamodb.domain.sample.PlaylistId;
import org.socialsignin.spring.data.dynamodb.domain.sample.User;
import org.socialsignin.spring.data.dynamodb.repository.QueryConstants;
import org.socialsignin.spring.data.dynamodb.repository.support.DynamoDBEntityInformation;
import org.socialsignin.spring.data.dynamodb.repository.support.DynamoDBIdIsHashAndRangeKeyEntityInformation;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.RepositoryQuery;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;
import static org.socialsignin.spring.data.dynamodb.repository.util.CollectionUtils.mapOf;

@RunWith(MockitoJUnitRunner.class)
public class PartTreeDynamoDBQueryUnitTest {

	private RepositoryQuery partTreeDynamoDBQuery;

	@Mock
	private DynamoDBOperations mockDynamoDBOperations;
	@Mock
	private DynamoDBQueryMethod<User, String> mockDynamoDBUserQueryMethod;
	@Mock
	private DynamoDBEntityInformation<User, String> mockUserEntityMetadata;
	@Mock
	private DynamoDBQueryMethod<Playlist, PlaylistId> mockDynamoDBPlaylistQueryMethod;
	@Mock
	private DynamoDBIdIsHashAndRangeKeyEntityInformation<Playlist, PlaylistId> mockPlaylistEntityMetadata;

	@SuppressWarnings("rawtypes")
	@Mock
	private Parameters mockParameters;

	@Mock
	private User mockUser;
	@Mock
	private Playlist mockPlaylist;

	@Mock
	private PaginatedScanList<User> mockUserScanResults;
	@Mock
	private PaginatedScanList<Playlist> mockPlaylistScanResults;
	@Mock
	private PaginatedQueryList<Playlist> mockPlaylistQueryResults;
	@Mock
	private PaginatedQueryList<User> mockUserQueryResults;

	// Mock out specific DynamoDBOperations behavior expected by this method
	ArgumentCaptor<DynamoDBQueryExpression<Playlist>> playlistQueryCaptor;
	ArgumentCaptor<QueryRequest> queryResultCaptor = ArgumentCaptor.forClass(QueryRequest.class);
	ArgumentCaptor<Class<Playlist>> playlistClassCaptor;
	ArgumentCaptor<DynamoDBQueryExpression<User>> userQueryCaptor;
	ArgumentCaptor<Class<User>> userClassCaptor;
	ArgumentCaptor<DynamoDBScanExpression> scanCaptor;

	@Before
	@SuppressWarnings("unchecked")
	public void setUp() {
        when(mockPlaylistEntityMetadata.isCompositeHashAndRangeKeyProperty("playlistId")).thenReturn(true);
        when(mockPlaylistEntityMetadata.getHashKeyPropertyName()).thenReturn("userName");
        // Mockito.when(mockPlaylistEntityMetadata.isHashKeyProperty("userName")).thenReturn(true);
        when(mockPlaylistEntityMetadata.getJavaType()).thenReturn(Playlist.class);
        when(mockPlaylistEntityMetadata.getRangeKeyPropertyName()).thenReturn("playlistName");
        when(mockPlaylistEntityMetadata.getIndexRangeKeyPropertyNames()).thenReturn(new HashSet<>());
        when(mockDynamoDBUserQueryMethod.getEntityInformation()).thenReturn(mockUserEntityMetadata);
        when(mockDynamoDBUserQueryMethod.getParameters()).thenReturn(mockParameters);
        when(mockDynamoDBUserQueryMethod.getConsistentReadMode()).thenReturn(QueryConstants.ConsistentReadMode.DEFAULT);
        when(mockDynamoDBPlaylistQueryMethod.getEntityInformation()).thenReturn(mockPlaylistEntityMetadata);
        when(mockDynamoDBPlaylistQueryMethod.getParameters()).thenReturn(mockParameters);
        when(mockUserEntityMetadata.getHashKeyPropertyName()).thenReturn("id");
        // Mockito.when(mockUserEntityMetadata.isHashKeyProperty("id")).thenReturn(true);
        when(mockUserEntityMetadata.getJavaType()).thenReturn(User.class);
        when(mockDynamoDBUserQueryMethod.isScanEnabled()).thenReturn(true);
        when(mockDynamoDBPlaylistQueryMethod.isScanEnabled()).thenReturn(true);

        // Mock out specific DynamoDBOperations behavior expected by this method
        playlistQueryCaptor = ArgumentCaptor.forClass(DynamoDBQueryExpression.class);
        playlistClassCaptor = ArgumentCaptor.forClass(Class.class);
        userClassCaptor = ArgumentCaptor.forClass(Class.class);
        scanCaptor = ArgumentCaptor.forClass(DynamoDBScanExpression.class);
    }

	private <T, ID extends Serializable> void setupCommonMocksForThisRepositoryMethod(
			DynamoDBEntityInformation<T, ID> mockEntityMetadata, DynamoDBQueryMethod<T, ID> mockDynamoDBQueryMethod,
			Class<T> clazz, String repositoryMethodName, int numberOfParameters, String hashKeyProperty,
			String rangeKeyProperty) {

        if (rangeKeyProperty != null) {
            when(mockEntityMetadata.isRangeKeyAware()).thenReturn(true);
        }


        when(mockDynamoDBQueryMethod.getEntityType()).thenReturn(clazz);
        when(mockDynamoDBQueryMethod.getName()).thenReturn(repositoryMethodName);
        when(mockDynamoDBQueryMethod.getParameters()).thenReturn(mockParameters);
        when(mockDynamoDBQueryMethod.getConsistentReadMode()).thenReturn(QueryConstants.ConsistentReadMode.DEFAULT);
        when(mockParameters.getBindableParameters()).thenReturn(mockParameters);
        when(mockParameters.getNumberOfParameters()).thenReturn(numberOfParameters);
        // Mockito.when(mockDynamoDBQueryMethod.getReturnedObjectType()).thenReturn(clazz);
        if (hashKeyProperty != null) {
            // Mockito.when(mockEntityMetadata.isHashKeyProperty(hashKeyProperty)).thenReturn(true);
        }

        for (int i = 0; i < numberOfParameters; i++) {
            Parameter mockParameter = Mockito.mock(Parameter.class);
            when(mockParameter.getIndex()).thenReturn(i);
            when(mockParameters.getBindableParameter(i)).thenReturn(mockParameter);
        }
        partTreeDynamoDBQuery = new PartTreeDynamoDBQuery<>(mockDynamoDBOperations, mockDynamoDBQueryMethod);
    }

	@Test
	public void testGetQueryMethod() {
		setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
				"findById", 1, "id", null);
		assertEquals(mockDynamoDBUserQueryMethod, partTreeDynamoDBQuery.getQueryMethod());
	}

	@Test
	public void testExecute_WhenFinderMethodIsFindingSingleEntity_WithSingleStringParameter_WhenFindingByHashKey() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findById", 1, "id", null);

        // Mock out specific DynamoDBOperations behavior expected by this method
        when(mockDynamoDBOperations.load(User.class, "someId")).thenReturn(mockUser);

        // Execute the query
        Object[] parameters = new Object[]{"someId"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(o, mockUser);

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).load(User.class, "someId");
    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingSingleEntityWithCompositeId_WithSingleStringParameter_WhenFindingByHashAndRangeKey() {
        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByUserNameAndPlaylistName", 2, "userName", "playlistName");

        // Mock out specific DynamoDBOperations behavior expected by this method
        when(mockDynamoDBOperations.load(Playlist.class, "someUserName", "somePlaylistName"))
                .thenReturn(mockPlaylist);

        // Execute the query
        Object[] parameters = new Object[]{"someUserName", "somePlaylistName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(o, mockPlaylist);

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).load(Playlist.class, "someUserName", "somePlaylistName");
    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingSingleEntityWithCompositeId_WhenFindingByCompositeId() {
        PlaylistId playlistId = new PlaylistId();
        playlistId.setUserName("someUserName");
        playlistId.setPlaylistName("somePlaylistName");

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistId", 1, "userName", "playlistName");
        when(mockPlaylistEntityMetadata.getHashKey(playlistId)).thenReturn("someUserName");
        when(mockPlaylistEntityMetadata.getRangeKey(playlistId)).thenReturn("somePlaylistName");

        // Mock out specific DynamoDBOperations behavior expected by this method
        when(mockDynamoDBOperations.load(Playlist.class, "someUserName", "somePlaylistName"))
                .thenReturn(mockPlaylist);

        // Execute the query

        Object[] parameters = new Object[]{playlistId};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(o, mockPlaylist);

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).load(Playlist.class, "someUserName", "somePlaylistName");
    }

	@Test(expected = UnsupportedOperationException.class)
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByNotCompositeId() {
        PlaylistId playlistId = new PlaylistId();
        playlistId.setUserName("someUserName");
        playlistId.setPlaylistName("somePlaylistName");

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistIdNot", 1, "userName", "playlistName");

        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        // Mockito.when(mockPlaylistEntityMetadata.getHashKey(playlistId)).thenReturn("someUserName");
        // Mockito.when(mockPlaylistEntityMetadata.getRangeKey(playlistId)).thenReturn("somePlaylistName");

        // Mock out specific DynamoDBOperations behavior expected by this method
        // ArgumentCaptor<DynamoDBScanExpression> scanCaptor =
        // ArgumentCaptor.forClass(DynamoDBScanExpression.class);
        // ArgumentCaptor<Class> classCaptor = ArgumentCaptor.forClass(Class.class);
        // Mockito.when(mockPlaylistScanResults.get(0)).thenReturn(mockPlaylist);
        // Mockito.when(mockPlaylistScanResults.size()).thenReturn(1);
        // Mockito.when(mockDynamoDBOperations.scan(classCaptor.capture(),
        // scanCaptor.capture())).thenReturn(
        // mockPlaylistScanResults);

        // Execute the query

        Object[] parameters = new Object[]{playlistId};
        partTreeDynamoDBQuery.execute(parameters);

    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByRangeKeyOnly() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistName", 1, "userName", "playlistName");

        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockPlaylistScanResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(playlistClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockPlaylistScanResults);

        // Execute the query

        Object[] parameters = new Object[]{"somePlaylistName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistScanResults, o);
        assertEquals(1, mockPlaylistScanResults.size());
        assertEquals(mockPlaylist, mockPlaylistScanResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        // Assert that we only one filter condition for the one property
        Map<String, Condition> filterConditions = scanCaptor.getValue().getScanFilter();
        assertEquals(1, filterConditions.size());
        Condition filterCondition = filterConditions.get("playlistName");

        assertNotNull(filterCondition);

        assertEquals(ComparisonOperator.EQ.name(), filterCondition.getComparisonOperator());

        // Assert we only have one attribute value for the filter condition
        assertEquals(1, filterCondition.getAttributeValueList().size());

        // Assert that there the attribute value type for this attribute value
        // is String,
		// and its value is the parameter expected
		assertEquals("somePlaylistName", filterCondition.getAttributeValueList().get(0).getS());

		// Assert that all other attribute value types other than String type
		// are null
		assertNull(filterCondition.getAttributeValueList().get(0).getSS());
		assertNull(filterCondition.getAttributeValueList().get(0).getN());
		assertNull(filterCondition.getAttributeValueList().get(0).getNS());
		assertNull(filterCondition.getAttributeValueList().get(0).getB());
		assertNull(filterCondition.getAttributeValueList().get(0).getBS());

		// Verify that the expected DynamoDBOperations method was called
		Mockito.verify(mockDynamoDBOperations).scan(playlistClassCaptor.getValue(), scanCaptor.getValue());
	}

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByCompositeIdWithRangeKeyOnly() {
        PlaylistId playlistId = new PlaylistId();
        playlistId.setPlaylistName("somePlaylistName");

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistId", 1, "userName", "playlistName");

        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        when(mockPlaylistEntityMetadata.getHashKey(playlistId)).thenReturn(null);
        when(mockPlaylistEntityMetadata.getRangeKey(playlistId)).thenReturn("somePlaylistName");

        when(mockPlaylistScanResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(playlistClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockPlaylistScanResults);

        // Execute the query

        Object[] parameters = new Object[]{playlistId};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistScanResults, o);
        assertEquals(1, mockPlaylistScanResults.size());
        assertEquals(mockPlaylist, mockPlaylistScanResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        // Assert that we only one filter condition for the one property
        Map<String, Condition> filterConditions = scanCaptor.getValue().getScanFilter();
        assertEquals(1, filterConditions.size());
        Condition filterCondition = filterConditions.get("playlistName");

        assertNotNull(filterCondition);

        assertEquals(ComparisonOperator.EQ.name(), filterCondition.getComparisonOperator());

        // Assert we only have one attribute value for the filter condition
        assertEquals(1, filterCondition.getAttributeValueList().size());

        // Assert that there the attribute value type for this attribute value
        // is String,
		// and its value is the parameter expected
		assertEquals("somePlaylistName", filterCondition.getAttributeValueList().get(0).getS());

		// Assert that all other attribute value types other than String type
		// are null
		assertNull(filterCondition.getAttributeValueList().get(0).getSS());
		assertNull(filterCondition.getAttributeValueList().get(0).getN());
		assertNull(filterCondition.getAttributeValueList().get(0).getNS());
		assertNull(filterCondition.getAttributeValueList().get(0).getB());
		assertNull(filterCondition.getAttributeValueList().get(0).getBS());

		// Verify that the expected DynamoDBOperations method was called
		Mockito.verify(mockDynamoDBOperations).scan(playlistClassCaptor.getValue(), scanCaptor.getValue());
	}

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WithSingleStringParameter_WhenFindingByHashKeyOnly() {
        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByUserName", 1, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName"))
                .thenReturn(prototypeHashKey);

        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), playlistQueryCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);

        // Execute the query
        Object[] parameters = new Object[]{"someUserName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        Playlist hashKeyPropertyPlaylist = playlistQueryCaptor.getValue().getHashKeyValues();
        assertNotNull(hashKeyPropertyPlaylist);
        assertEquals("someUserName", hashKeyPropertyPlaylist.getUserName());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), playlistQueryCaptor.getValue());
    }

	@Test
	public void testExecute_WhenFinderMethodIsCountingEntityWithCompositeIdList_WhenFindingByRangeKeyOnly_ScanCountEnabled() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "countByPlaylistName", 1, "userName", "playlistName");

        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(false);
        when(mockDynamoDBPlaylistQueryMethod.isScanCountEnabled()).thenReturn(true);

        when(mockDynamoDBOperations.count(playlistClassCaptor.capture(), scanCaptor.capture())).thenReturn(100);

        // Execute the query

        Object[] parameters = new Object[]{"somePlaylistName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(100L, o);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        // Assert that we only one filter condition for the one property
        Map<String, Condition> filterConditions = scanCaptor.getValue().getScanFilter();
        assertEquals(1, filterConditions.size());
        Condition filterCondition = filterConditions.get("playlistName");

        assertNotNull(filterCondition);

        assertEquals(ComparisonOperator.EQ.name(), filterCondition.getComparisonOperator());

        // Assert we only have one attribute value for the filter condition
        assertEquals(1, filterCondition.getAttributeValueList().size());

        // Assert that there the attribute value type for this attribute value
        // is String,
        // and its value is the parameter expected
        assertEquals("somePlaylistName", filterCondition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(filterCondition.getAttributeValueList().get(0).getSS());
        assertNull(filterCondition.getAttributeValueList().get(0).getN());
        assertNull(filterCondition.getAttributeValueList().get(0).getNS());
        assertNull(filterCondition.getAttributeValueList().get(0).getB());
        assertNull(filterCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).count(playlistClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test(expected = IllegalArgumentException.class)
	public void testExecute_WhenFinderMethodIsCountingEntityWithCompositeIdList_WhenFindingByRangeKeyOnly_ScanCountDisabled() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "countByPlaylistName", 1, "userName", "playlistName");

        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(false);
        when(mockDynamoDBPlaylistQueryMethod.isScanCountEnabled()).thenReturn(false);

        // Mock out specific DynamoDBOperations behavior expected by this method
        // ArgumentCaptor<DynamoDBScanExpression> scanCaptor =
        // ArgumentCaptor.forClass(DynamoDBScanExpression.class);
        // ArgumentCaptor<Class<Playlist>> classCaptor =
        // ArgumentCaptor.forClass(Class.class);
        // Mockito.when(mockDynamoDBOperations.count(classCaptor.capture(),
        // scanCaptor.capture())).thenReturn(
        // 100);

        // Execute the query

        Object[] parameters = new Object[]{"somePlaylistName"};
        partTreeDynamoDBQuery.execute(parameters);

    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WithSingleStringParameter_WhenFindingByHashKeyAndNotRangeKey() {
        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByUserNameAndPlaylistNameNot", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");

        // Mockito.when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName")).thenReturn(
        // prototypeHashKey);

        when(mockPlaylistScanResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(playlistClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockPlaylistScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someUserName", "somePlaylistName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistScanResults, o);
        assertEquals(1, mockPlaylistScanResults.size());
        assertEquals(mockPlaylist, mockPlaylistScanResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        // Assert that we have the correct filter conditions
        Map<String, Condition> filterConditions = scanCaptor.getValue().getScanFilter();
        assertEquals(2, filterConditions.size());
        Condition filterCondition1 = filterConditions.get("userName");
        Condition filterCondition2 = filterConditions.get("playlistName");

        assertEquals(ComparisonOperator.EQ.name(), filterCondition1.getComparisonOperator());
        assertEquals(ComparisonOperator.NE.name(), filterCondition2.getComparisonOperator());

        // Assert we only have one attribute value for this filter condition
        assertEquals(1, filterCondition1.getAttributeValueList().size());
        assertEquals(1, filterCondition2.getAttributeValueList().size());

        // Assert that there the attribute value type for this attribute value
        // is String,
		// and its value is the parameter expected
		assertEquals("someUserName", filterCondition1.getAttributeValueList().get(0).getS());
		assertEquals("somePlaylistName", filterCondition2.getAttributeValueList().get(0).getS());

		// Assert that all other attribute value types other than String type
		// are null
		assertNull(filterCondition1.getAttributeValueList().get(0).getSS());
		assertNull(filterCondition1.getAttributeValueList().get(0).getN());
		assertNull(filterCondition1.getAttributeValueList().get(0).getNS());
		assertNull(filterCondition1.getAttributeValueList().get(0).getB());
		assertNull(filterCondition1.getAttributeValueList().get(0).getBS());

		assertNull(filterCondition2.getAttributeValueList().get(0).getSS());
		assertNull(filterCondition2.getAttributeValueList().get(0).getN());
		assertNull(filterCondition2.getAttributeValueList().get(0).getNS());
		assertNull(filterCondition2.getAttributeValueList().get(0).getB());
		assertNull(filterCondition2.getAttributeValueList().get(0).getBS());

		// Verify that the expected DynamoDBOperations method was called
		Mockito.verify(mockDynamoDBOperations).scan(playlistClassCaptor.getValue(), scanCaptor.getValue()); // Assert
		// that
		// we
		// obtain
		// the
		// expected
		// results

	}

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByCompositeIdWithHashKeyOnly() {
        PlaylistId playlistId = new PlaylistId();
        playlistId.setUserName("someUserName");

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistId", 1, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName"))
                .thenReturn(prototypeHashKey);
        when(mockPlaylistEntityMetadata.getHashKey(playlistId)).thenReturn("someUserName");
        when(mockPlaylistEntityMetadata.getRangeKey(playlistId)).thenReturn(null);

        // Mock out specific DynamoDBOperations behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), playlistQueryCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);

        // Execute the query
        Object[] parameters = new Object[]{playlistId};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        Playlist hashKeyPropertyPlaylist = playlistQueryCaptor.getValue().getHashKeyValues();
        assertNotNull(hashKeyPropertyPlaylist);
        assertEquals("someUserName", hashKeyPropertyPlaylist.getUserName());

        assertEquals(0, playlistQueryCaptor.getValue().getRangeKeyConditions().size());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), playlistQueryCaptor.getValue());

    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByCompositeId_HashKey() {
        PlaylistId playlistId = new PlaylistId();
        playlistId.setUserName("someUserName");

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistIdUserName", 1, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName"))
                .thenReturn(prototypeHashKey);
        // Mockito.when(mockPlaylistEntityMetadata.getHashKey(playlistId)).thenReturn("someUserName");
        // Mockito.when(mockPlaylistEntityMetadata.getRangeKey(playlistId)).thenReturn(null);

        // Mock out specific DynamoDBOperations behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), playlistQueryCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);

        // Execute the query
        Object[] parameters = new Object[]{"someUserName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        Playlist hashKeyPropertyPlaylist = playlistQueryCaptor.getValue().getHashKeyValues();
        assertNotNull(hashKeyPropertyPlaylist);
        assertEquals("someUserName", hashKeyPropertyPlaylist.getUserName());

        assertEquals(0, playlistQueryCaptor.getValue().getRangeKeyConditions().size());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), playlistQueryCaptor.getValue());

    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByCompositeId_HashKeyAndIndexRangeKey() {
        PlaylistId playlistId = new PlaylistId();
        playlistId.setUserName("someUserName");

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistIdUserNameAndDisplayName", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName"))
                .thenReturn(prototypeHashKey);
        // Mockito.when(mockPlaylistEntityMetadata.getHashKey(playlistId)).thenReturn("someUserName");
        // Mockito.when(mockPlaylistEntityMetadata.getRangeKey(playlistId)).thenReturn(null);
        Set<String> indexRangeKeyPropertyNames = new HashSet<>();
        indexRangeKeyPropertyNames.add("displayName");
        when(mockPlaylistEntityMetadata.getIndexRangeKeyPropertyNames()).thenReturn(indexRangeKeyPropertyNames);

        // Mock out specific DynamoDBOperations behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), playlistQueryCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);

        // Execute the query
        Object[] parameters = new Object[]{"someUserName", "someDisplayName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        // Assert that we have two filter condition, for the name of the
        // property
        Playlist hashKeyPrototypeObject = playlistQueryCaptor.getValue().getHashKeyValues();
        assertNotNull(hashKeyPrototypeObject);
        assertEquals("someUserName", hashKeyPrototypeObject.getUserName());

        assertEquals(1, playlistQueryCaptor.getValue().getRangeKeyConditions().size());

        Condition condition = playlistQueryCaptor.getValue().getRangeKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.EQ.name(), condition.getComparisonOperator());
        assertEquals(1, condition.getAttributeValueList().size());
        assertEquals("someDisplayName", condition.getAttributeValueList().get(0).getS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), playlistQueryCaptor.getValue());

    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingSingleEntityWithCompositeId_WhenFindingByCompositeId_HashKeyAndCompositeId_RangeKey() {
        PlaylistId playlistId = new PlaylistId();
        playlistId.setUserName("someUserName");
        playlistId.setPlaylistName("somePlaylistName");

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistIdUserNameAndPlaylistIdPlaylistName", 2, "userName", "playlistName");
        // Mockito.when(mockPlaylistEntityMetadata.getHashKey(playlistId)).thenReturn("someUserName");
        // Mockito.when(mockPlaylistEntityMetadata.getRangeKey(playlistId)).thenReturn("somePlaylistName");

        // Mock out specific DynamoDBOperations behavior expected by this method
        when(mockDynamoDBOperations.load(Playlist.class, "someUserName", "somePlaylistName"))
                .thenReturn(mockPlaylist);

        // Execute the query

        Object[] parameters = new Object[]{"someUserName", "somePlaylistName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(o, mockPlaylist);

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).load(Playlist.class, "someUserName", "somePlaylistName");
    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByCompositeIdWithHashKeyOnly_WhenSortingByRangeKey() {
        PlaylistId playlistId = new PlaylistId();
        playlistId.setUserName("someUserName");

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistIdOrderByPlaylistNameDesc", 1, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName"))
                .thenReturn(prototypeHashKey);
        when(mockPlaylistEntityMetadata.getHashKey(playlistId)).thenReturn("someUserName");
        when(mockPlaylistEntityMetadata.getRangeKey(playlistId)).thenReturn(null);

        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), playlistQueryCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);

        // Execute the query
        Object[] parameters = new Object[]{playlistId};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        Playlist hashKeyPropertyPlaylist = playlistQueryCaptor.getValue().getHashKeyValues();
        assertNotNull(hashKeyPropertyPlaylist);
        assertEquals("someUserName", hashKeyPropertyPlaylist.getUserName());

        assertEquals(0, playlistQueryCaptor.getValue().getRangeKeyConditions().size());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), playlistQueryCaptor.getValue());

    }

	// Can't sort by indexrangekey when querying by hash key only
	@Test(expected = UnsupportedOperationException.class)
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByCompositeIdWithHashKeyOnly_WhenSortingByIndexRangeKey() {
        PlaylistId playlistId = new PlaylistId();
        playlistId.setUserName("someUserName");

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistIdOrderByDisplayNameDesc", 1, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName"))
                .thenReturn(prototypeHashKey);
        when(mockPlaylistEntityMetadata.getHashKey(playlistId)).thenReturn("someUserName");
        when(mockPlaylistEntityMetadata.getRangeKey(playlistId)).thenReturn(null);
        Set<String> indexRangeKeyPropertyNames = new HashSet<>();
        indexRangeKeyPropertyNames.add("displayName");
        when(mockPlaylistEntityMetadata.getIndexRangeKeyPropertyNames()).thenReturn(indexRangeKeyPropertyNames);

        // Mock out specific DynamoDBOperations behavior expected by this method
        // ArgumentCaptor<DynamoDBQueryExpression> queryCaptor =
        // ArgumentCaptor.forClass(DynamoDBQueryExpression.class);
        // ArgumentCaptor<Class> classCaptor = ArgumentCaptor.forClass(Class.class);
        // Mockito.when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        // Mockito.when(mockPlaylistQueryResults.size()).thenReturn(1);
        // Mockito.when(mockDynamoDBOperations.query(classCaptor.capture(),
        // queryCaptor.capture())).thenReturn(
        // mockPlaylistQueryResults);

        // Execute the query
        Object[] parameters = new Object[]{playlistId};
        partTreeDynamoDBQuery.execute(parameters);

    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByHashKeyAndIndexRangeKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByUserNameAndDisplayName", 2, "userName", "playlistName");
        Set<String> indexRangeKeyPropertyNames = new HashSet<>();
        indexRangeKeyPropertyNames.add("displayName");
        when(mockPlaylistEntityMetadata.getIndexRangeKeyPropertyNames()).thenReturn(indexRangeKeyPropertyNames);
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName"))
                .thenReturn(prototypeHashKey);

        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), playlistQueryCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);

        // Execute the query
        Object[] parameters = new Object[]{"someUserName", "someDisplayName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        // Assert that we have only one filter condition for the hash key,and for the
        // index range key
        Playlist hashKeyPropertyPlaylist = playlistQueryCaptor.getValue().getHashKeyValues();
        assertEquals("someUserName", hashKeyPropertyPlaylist.getUserName());

        assertEquals(1, playlistQueryCaptor.getValue().getRangeKeyConditions().size());
        Condition condition = playlistQueryCaptor.getValue().getRangeKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.EQ.name(), condition.getComparisonOperator());
        assertEquals(1, condition.getAttributeValueList().size());
        assertEquals("someDisplayName", condition.getAttributeValueList().get(0).getS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), playlistQueryCaptor.getValue());

    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByHashKeyAndIndexRangeKey_WithValidOrderSpecified() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByUserNameAndDisplayNameOrderByDisplayNameDesc", 2, "userName", "playlistName");
        Set<String> indexRangeKeyPropertyNames = new HashSet<>();
        indexRangeKeyPropertyNames.add("displayName");
        when(mockPlaylistEntityMetadata.getIndexRangeKeyPropertyNames()).thenReturn(indexRangeKeyPropertyNames);
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName"))
                .thenReturn(prototypeHashKey);

        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), playlistQueryCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);

        // Execute the query
        Object[] parameters = new Object[]{"someUserName", "someDisplayName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        // Assert that we have only one filter condition for the hash key,and for the
        // index range key
        Playlist hashKeyPropertyPlaylist = playlistQueryCaptor.getValue().getHashKeyValues();
        assertNotNull(hashKeyPropertyPlaylist);
        assertEquals("someUserName", hashKeyPropertyPlaylist.getUserName());

        assertEquals(1, playlistQueryCaptor.getValue().getRangeKeyConditions().size());
        Condition condition = playlistQueryCaptor.getValue().getRangeKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.EQ.name(), condition.getComparisonOperator());
        assertEquals(1, condition.getAttributeValueList().size());
        assertEquals("someDisplayName", condition.getAttributeValueList().get(0).getS());

        Assert.assertFalse(playlistQueryCaptor.getValue().isScanIndexForward());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), playlistQueryCaptor.getValue());

    }

	@Test(expected = UnsupportedOperationException.class)
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByHashKeyAndIndexRangeKey_WithInvalidOrderSpecified() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByUserNameAndDisplayNameOrderByPlaylistNameDesc", 2, "userName", "playlistName");
        Set<String> indexRangeKeyPropertyNames = new HashSet<>();
        indexRangeKeyPropertyNames.add("displayName");
        when(mockPlaylistEntityMetadata.getIndexRangeKeyPropertyNames()).thenReturn(indexRangeKeyPropertyNames);
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName"))
                .thenReturn(prototypeHashKey);

        // Mock out specific DynamoDBOperations behavior expected by this method
        // ArgumentCaptor<DynamoDBQueryExpression<Playlist>> queryCaptor =
        // ArgumentCaptor.forClass(DynamoDBQueryExpression.class);
        // ArgumentCaptor<Class<Playlist>> classCaptor =
        // ArgumentCaptor.forClass(Class.class);
        // Mockito.when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        // Mockito.when(mockPlaylistQueryResults.size()).thenReturn(1);
        // Mockito.when(mockDynamoDBOperations.query(classCaptor.capture(),
        // queryCaptor.capture())).thenReturn(
        // mockPlaylistQueryResults);

        // Execute the query
        Object[] parameters = new Object[]{"someUserName", "someDisplayName"};
        partTreeDynamoDBQuery.execute(parameters);

    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByHashKeyAndIndexRangeKey_OrderByIndexRangeKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByUserNameAndDisplayNameOrderByDisplayNameDesc", 2, "userName", "playlistName");
        Set<String> indexRangeKeyPropertyNames = new HashSet<>();
        indexRangeKeyPropertyNames.add("displayName");
        when(mockPlaylistEntityMetadata.getIndexRangeKeyPropertyNames()).thenReturn(indexRangeKeyPropertyNames);
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName"))
                .thenReturn(prototypeHashKey);

        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), playlistQueryCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);

        // Execute the query
        Object[] parameters = new Object[]{"someUserName", "someDisplayName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        // Assert that we have only one filter condition for the hash key,and for the
        // index range key
        Playlist hashKeyPropertyPlaylist = playlistQueryCaptor.getValue().getHashKeyValues();
        assertEquals("someUserName", hashKeyPropertyPlaylist.getUserName());

        assertEquals(1, playlistQueryCaptor.getValue().getRangeKeyConditions().size());
        Condition condition = playlistQueryCaptor.getValue().getRangeKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.EQ.name(), condition.getComparisonOperator());
        assertEquals(1, condition.getAttributeValueList().size());
        assertEquals("someDisplayName", condition.getAttributeValueList().get(0).getS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), playlistQueryCaptor.getValue());

    }

	// Sorting by range key when querying by indexrangekey not supported
	@Test(expected = UnsupportedOperationException.class)
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByHashKeyAndIndexRangeKey_OrderByRangeKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByUserNameAndDisplayNameOrderByPlaylistNameDesc", 2, "userName", "playlistName");
        Set<String> indexRangeKeyPropertyNames = new HashSet<>();
        indexRangeKeyPropertyNames.add("displayName");
        when(mockPlaylistEntityMetadata.getIndexRangeKeyPropertyNames()).thenReturn(indexRangeKeyPropertyNames);
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName"))
                .thenReturn(prototypeHashKey);

        // Mock out specific DynamoDBOperations behavior expected by this method
        // ArgumentCaptor<DynamoDBQueryExpression<Playlist>> queryCaptor =
        // ArgumentCaptor.forClass(DynamoDBQueryExpression.class);
        // ArgumentCaptor<Class<Playlist>> classCaptor =
        // ArgumentCaptor.forClass(Class.class);
        // Mockito.when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        // Mockito.when(mockPlaylistQueryResults.size()).thenReturn(1);
        // Mockito.when(mockDynamoDBOperations.query(classCaptor.capture(),
        // queryCaptor.capture())).thenReturn(
        // mockPlaylistQueryResults);

        // Execute the query
        Object[] parameters = new Object[]{"someUserName", "someDisplayName"};
        partTreeDynamoDBQuery.execute(parameters);

    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByHashKeyAndIndexRangeKeyWithOveriddenName() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByUserNameAndDisplayName", 2, "userName", "playlistName");
        Set<String> indexRangeKeyPropertyNames = new HashSet<>();
        indexRangeKeyPropertyNames.add("displayName");
        when(mockPlaylistEntityMetadata.getIndexRangeKeyPropertyNames()).thenReturn(indexRangeKeyPropertyNames);
        when(mockPlaylistEntityMetadata.getOverriddenAttributeName("displayName"))
                .thenReturn(Optional.of("DisplayName"));

        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName"))
                .thenReturn(prototypeHashKey);

        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), playlistQueryCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);

        // Execute the query
        Object[] parameters = new Object[]{"someUserName", "someDisplayName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        // Assert that we have only one filter condition for the hash key,and for the
        // index range key
        Playlist hashKeyPropertyPlaylist = playlistQueryCaptor.getValue().getHashKeyValues();
        assertEquals("someUserName", hashKeyPropertyPlaylist.getUserName());

        assertEquals(1, playlistQueryCaptor.getValue().getRangeKeyConditions().size());
        Condition condition = playlistQueryCaptor.getValue().getRangeKeyConditions().get("DisplayName");
        assertEquals(ComparisonOperator.EQ.name(), condition.getComparisonOperator());
        assertEquals(1, condition.getAttributeValueList().size());
        assertEquals("someDisplayName", condition.getAttributeValueList().get(0).getS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), playlistQueryCaptor.getValue());

    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByCompositeIdWithHashKeyOnlyAndByAnotherPropertyWithOverriddenAttributeName() {
        PlaylistId playlistId = new PlaylistId();
        playlistId.setUserName("someUserName");

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistIdAndDisplayName", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        // Mockito.when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName")).thenReturn(
        // prototypeHashKey);
        when(mockPlaylistEntityMetadata.getHashKey(playlistId)).thenReturn("someUserName");
        when(mockPlaylistEntityMetadata.getRangeKey(playlistId)).thenReturn(null);
        when(mockPlaylistEntityMetadata.getOverriddenAttributeName("displayName"))
                .thenReturn(Optional.of("DisplayName"));

        // Mockito.when(mockUserScanResults.get(0)).thenReturn(mockUser);
        // Mockito.when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{playlistId, "someDisplayName"};
        partTreeDynamoDBQuery.execute(parameters);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), Playlist.class);

        // Assert that we have only three filter conditions
        Map<String, Condition> filterConditions = scanCaptor.getValue().getScanFilter();
        assertEquals(2, filterConditions.size());
        Condition filterCondition1 = filterConditions.get("userName");
        Condition filterCondition2 = filterConditions.get("DisplayName");

        assertEquals(ComparisonOperator.EQ.name(), filterCondition1.getComparisonOperator());
        assertEquals(ComparisonOperator.EQ.name(), filterCondition2.getComparisonOperator());

        // Assert we only have one attribute value for this filter condition
        assertEquals(1, filterCondition1.getAttributeValueList().size());
        assertEquals(1, filterCondition2.getAttributeValueList().size());

        // Assert that there the attribute value type for this attribute value
        // is String,
        // and its value is the parameter expected
        assertEquals("someUserName", filterCondition1.getAttributeValueList().get(0).getS());
        assertEquals("someDisplayName", filterCondition2.getAttributeValueList().get(0).getS());

		// Assert that all other attribute value types other than String type
		// are null
		assertNull(filterCondition1.getAttributeValueList().get(0).getSS());
		assertNull(filterCondition1.getAttributeValueList().get(0).getN());
		assertNull(filterCondition1.getAttributeValueList().get(0).getNS());
		assertNull(filterCondition1.getAttributeValueList().get(0).getB());
		assertNull(filterCondition1.getAttributeValueList().get(0).getBS());

		assertNull(filterCondition2.getAttributeValueList().get(0).getSS());
		assertNull(filterCondition2.getAttributeValueList().get(0).getN());
		assertNull(filterCondition2.getAttributeValueList().get(0).getNS());
		assertNull(filterCondition2.getAttributeValueList().get(0).getB());
		assertNull(filterCondition2.getAttributeValueList().get(0).getBS());

		// Verify that the expected DynamoDBOperations method was called
		Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue()); // Assert
		// that we obtain the expected results
	}

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByCompositeIdWithRangeKeyOnlyAndByAnotherPropertyWithOverriddenAttributeName() {
        PlaylistId playlistId = new PlaylistId();
        playlistId.setPlaylistName("somePlaylistName");

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistIdAndDisplayName", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");
        when(mockPlaylistEntityMetadata.getHashKey(playlistId)).thenReturn(null);
        when(mockPlaylistEntityMetadata.getRangeKey(playlistId)).thenReturn("somePlaylistName");
        when(mockPlaylistEntityMetadata.getOverriddenAttributeName("displayName"))
                .thenReturn(Optional.of("DisplayName"));

        // Mockito.when(mockUserScanResults.get(0)).thenReturn(mockUser);
        // Mockito.when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{playlistId, "someDisplayName"};
        partTreeDynamoDBQuery.execute(parameters);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), Playlist.class);

        // Assert that we have only three filter conditions
        Map<String, Condition> filterConditions = scanCaptor.getValue().getScanFilter();
        assertEquals(2, filterConditions.size());
        Condition filterCondition1 = filterConditions.get("playlistName");
        Condition filterCondition2 = filterConditions.get("DisplayName");

        assertEquals(ComparisonOperator.EQ.name(), filterCondition1.getComparisonOperator());
        assertEquals(ComparisonOperator.EQ.name(), filterCondition2.getComparisonOperator());

        // Assert we only have one attribute value for this filter condition
        assertEquals(1, filterCondition1.getAttributeValueList().size());
        assertEquals(1, filterCondition2.getAttributeValueList().size());

        // Assert that there the attribute value type for this attribute value
        // is String,
        // and its value is the parameter expected
        assertEquals("somePlaylistName", filterCondition1.getAttributeValueList().get(0).getS());
        assertEquals("someDisplayName", filterCondition2.getAttributeValueList().get(0).getS());

		// Assert that all other attribute value types other than String type
		// are null
		assertNull(filterCondition1.getAttributeValueList().get(0).getSS());
		assertNull(filterCondition1.getAttributeValueList().get(0).getN());
		assertNull(filterCondition1.getAttributeValueList().get(0).getNS());
		assertNull(filterCondition1.getAttributeValueList().get(0).getB());
		assertNull(filterCondition1.getAttributeValueList().get(0).getBS());

		assertNull(filterCondition2.getAttributeValueList().get(0).getSS());
		assertNull(filterCondition2.getAttributeValueList().get(0).getN());
		assertNull(filterCondition2.getAttributeValueList().get(0).getNS());
		assertNull(filterCondition2.getAttributeValueList().get(0).getB());
		assertNull(filterCondition2.getAttributeValueList().get(0).getBS());

		// Verify that the expected DynamoDBOperations method was called
		Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue()); // Assert
		// that we obtain the expected results
	}

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByNotHashKeyAndNotRangeKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByUserNameNotAndPlaylistNameNot", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        // Mockito.when(mockUserScanResults.get(0)).thenReturn(mockUser);
        // Mockito.when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someUserName", "somePlaylistName"};
        partTreeDynamoDBQuery.execute(parameters);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), Playlist.class);

        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(2, names.size());
        assertEquals(mapOf("#key1", "userName", "#key2", "playlistName"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(2, values.size());
        assertEquals(mapOf(
                ":value1", new AttributeValue().withS("someUserName"),
                ":value2", new AttributeValue().withS("somePlaylistName")),
                values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 <> :value1 AND #key2 <> :value2"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue()); // Assert
        // that we obtain the expected results
    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeIdList_WhenFindingByCompositeIdAndByAnotherPropertyWithOverriddenAttributeName() {
        PlaylistId playlistId = new PlaylistId();
        playlistId.setUserName("someUserName");
        playlistId.setPlaylistName("somePlaylistName");

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistIdAndDisplayName", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);
        Playlist prototypeHashKey = new Playlist();
        prototypeHashKey.setUserName("someUserName");

        // Mockito.when(mockPlaylistEntityMetadata.getHashKeyPropotypeEntityForHashKey("someUserName")).thenReturn(
        // prototypeHashKey);
        when(mockPlaylistEntityMetadata.getHashKey(playlistId)).thenReturn("someUserName");
        when(mockPlaylistEntityMetadata.getRangeKey(playlistId)).thenReturn("somePlaylistName");
        when(mockPlaylistEntityMetadata.getOverriddenAttributeName("displayName"))
                .thenReturn(Optional.of("DisplayName"));

        // Mockito.when(mockUserScanResults.get(0)).thenReturn(mockUser);
        // Mockito.when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{playlistId, "someDisplayName"};
        partTreeDynamoDBQuery.execute(parameters);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), Playlist.class);

        // Assert that we have only three filter conditions
        Map<String, Condition> filterConditions = scanCaptor.getValue().getScanFilter();

        //TODO filter expression
        assertEquals(3, filterConditions.size());
        Condition filterCondition1 = filterConditions.get("userName");
        Condition filterCondition2 = filterConditions.get("playlistName");
        Condition filterCondition3 = filterConditions.get("DisplayName");

        assertEquals(ComparisonOperator.EQ.name(), filterCondition1.getComparisonOperator());
        assertEquals(ComparisonOperator.EQ.name(), filterCondition2.getComparisonOperator());
        assertEquals(ComparisonOperator.EQ.name(), filterCondition3.getComparisonOperator());

        // Assert we only have one attribute value for this filter condition
        assertEquals(1, filterCondition1.getAttributeValueList().size());
        assertEquals(1, filterCondition2.getAttributeValueList().size());
        assertEquals(1, filterCondition3.getAttributeValueList().size());

        // Assert that there the attribute value type for this attribute value
        // is String,
        // and its value is the parameter expected
        assertEquals("someUserName", filterCondition1.getAttributeValueList().get(0).getS());
        assertEquals("somePlaylistName", filterCondition2.getAttributeValueList().get(0).getS());
        assertEquals("someDisplayName", filterCondition3.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(filterCondition1.getAttributeValueList().get(0).getSS());
        assertNull(filterCondition1.getAttributeValueList().get(0).getN());
        assertNull(filterCondition1.getAttributeValueList().get(0).getNS());
        assertNull(filterCondition1.getAttributeValueList().get(0).getB());
        assertNull(filterCondition1.getAttributeValueList().get(0).getBS());

        assertNull(filterCondition2.getAttributeValueList().get(0).getSS());
        assertNull(filterCondition2.getAttributeValueList().get(0).getN());
        assertNull(filterCondition2.getAttributeValueList().get(0).getNS());
        assertNull(filterCondition2.getAttributeValueList().get(0).getB());
        assertNull(filterCondition2.getAttributeValueList().get(0).getBS());

        assertNull(filterCondition3.getAttributeValueList().get(0).getSS());
        assertNull(filterCondition3.getAttributeValueList().get(0).getN());
        assertNull(filterCondition3.getAttributeValueList().get(0).getNS());
        assertNull(filterCondition3.getAttributeValueList().get(0).getB());
        assertNull(filterCondition3.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue()); // Assert
        // that we obtain the expected results
    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingSingleEntity_WithSingleStringParameter_WhenNotFindingByHashKey() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByName", 1, "id", null);

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(o, mockUser);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "name"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withS("someName")), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 = :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingSingleEntity_WithSingleStringParameter_WhenNotFindingByHashKey_WhenDynamoAttributeNameOverridden() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByName", 1, "id", null);
        when(mockUserEntityMetadata.getOverriddenAttributeName("name")).thenReturn(Optional.of("Name"));

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(o, mockUser);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "name"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withS("someName")), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 = :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingSingleEntity_WithMultipleStringParameters_WhenFindingByHashKeyAndANonHashOrRangeProperty() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByIdAndName", 2, "id", null);

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someId", "someName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(o, mockUser);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have two filter conditions, for the id and name
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, Condition> filterConditions = scanExpression.getScanFilter();

        //TODO filter expression
        assertEquals(2, filterConditions.size());
        Condition nameFilterCondition = filterConditions.get("name");
        assertNotNull(nameFilterCondition);
        Condition idFilterCondition = filterConditions.get("id");
        assertNotNull(idFilterCondition);

        assertEquals(ComparisonOperator.EQ.name(), nameFilterCondition.getComparisonOperator());
        assertEquals(ComparisonOperator.EQ.name(), idFilterCondition.getComparisonOperator());

        // Assert we only have one attribute value for each filter condition
        assertEquals(1, nameFilterCondition.getAttributeValueList().size());
        assertEquals(1, idFilterCondition.getAttributeValueList().size());

        // Assert that there the attribute value type for this attribute value
        // is String,
        // and its value is the parameter expected
        assertEquals("someName", nameFilterCondition.getAttributeValueList().get(0).getS());
        assertEquals("someId", idFilterCondition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(nameFilterCondition.getAttributeValueList().get(0).getSS());
        assertNull(nameFilterCondition.getAttributeValueList().get(0).getN());
        assertNull(nameFilterCondition.getAttributeValueList().get(0).getNS());
        assertNull(nameFilterCondition.getAttributeValueList().get(0).getB());
        assertNull(nameFilterCondition.getAttributeValueList().get(0).getBS());
        assertNull(idFilterCondition.getAttributeValueList().get(0).getSS());
        assertNull(idFilterCondition.getAttributeValueList().get(0).getN());
        assertNull(idFilterCondition.getAttributeValueList().get(0).getNS());
        assertNull(idFilterCondition.getAttributeValueList().get(0).getB());
        assertNull(idFilterCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingSingleEntity_WithMultipleStringParameters_WhenFindingByHashKeyAndACollectionProperty() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByTestSet", 1, "id", null);

        Set<String> testSet = new HashSet<>();
        testSet.add("testData");

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{testSet};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(o, mockUser);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have one filter condition
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "testSet"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withSS("testData")), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 = :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingSingleEntity_WithMultipleStringParameters_WhenFindingByHashKeyAndANonHashOrRangeProperty_WhenDynamoDBAttributeNamesOveridden() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByIdAndName", 2, "id", null);

        when(mockUserEntityMetadata.getOverriddenAttributeName("name")).thenReturn(Optional.of("Name"));
        when(mockUserEntityMetadata.getOverriddenAttributeName("id")).thenReturn(Optional.of("Id"));

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someId", "someName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(o, mockUser);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have two filter conditions, for the id and name
        Map<String, Condition> filterConditions = scanCaptor.getValue().getScanFilter();
        assertEquals(2, filterConditions.size());
        Condition nameFilterCondition = filterConditions.get("Name");
        assertNotNull(nameFilterCondition);
        Condition idFilterCondition = filterConditions.get("Id");
        assertNotNull(idFilterCondition);

        assertEquals(ComparisonOperator.EQ.name(), nameFilterCondition.getComparisonOperator());
        assertEquals(ComparisonOperator.EQ.name(), idFilterCondition.getComparisonOperator());

        // Assert we only have one attribute value for each filter condition
        assertEquals(1, nameFilterCondition.getAttributeValueList().size());
        assertEquals(1, idFilterCondition.getAttributeValueList().size());

        // Assert that there the attribute value type for this attribute value
		// is String,
		// and its value is the parameter expected
		assertEquals("someName", nameFilterCondition.getAttributeValueList().get(0).getS());
		assertEquals("someId", idFilterCondition.getAttributeValueList().get(0).getS());

		// Assert that all other attribute value types other than String type
		// are null
		assertNull(nameFilterCondition.getAttributeValueList().get(0).getSS());
		assertNull(nameFilterCondition.getAttributeValueList().get(0).getN());
		assertNull(nameFilterCondition.getAttributeValueList().get(0).getNS());
		assertNull(nameFilterCondition.getAttributeValueList().get(0).getB());
		assertNull(nameFilterCondition.getAttributeValueList().get(0).getBS());
		assertNull(idFilterCondition.getAttributeValueList().get(0).getSS());
		assertNull(idFilterCondition.getAttributeValueList().get(0).getN());
		assertNull(idFilterCondition.getAttributeValueList().get(0).getNS());
		assertNull(idFilterCondition.getAttributeValueList().get(0).getB());
		assertNull(idFilterCondition.getAttributeValueList().get(0).getBS());

		// Verify that the expected DynamoDBOperations method was called
		Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
	}

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityList_WithSingleStringParameter_WhenNotFindingByHashKey() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByName", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected list of results
        assertEquals(o, mockUserScanResults);

        // Assert that the list of results contains the correct elements
        assertEquals(1, mockUserScanResults.size());
        assertEquals(mockUser, mockUserScanResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "name"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withS("someName")), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 = :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test(expected = UnsupportedOperationException.class)
	// Not yet supported
	public void testExecute_WhenFinderMethodIsFindingEntityList_WithSingleStringParameterIgnoringCase_WhenNotFindingByHashKey() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByNameIgnoringCase", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);

        // Mock out specific DynamoDBOperations behavior expected by this method
        // ArgumentCaptor<DynamoDBScanExpression> scanCaptor =
        // ArgumentCaptor.forClass(DynamoDBScanExpression.class);
        // ArgumentCaptor<Class<User>> classCaptor =
        // ArgumentCaptor.forClass(Class.class);
        // Mockito.when(mockUserScanResults.get(0)).thenReturn(mockUser);
        // Mockito.when(mockUserScanResults.size()).thenReturn(1);
        // Mockito.when(mockDynamoDBOperations.scan(classCaptor.capture(),
        // scanCaptor.capture())).thenReturn(
        // mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someName"};
        partTreeDynamoDBQuery.execute(parameters);

    }

	@Test(expected = UnsupportedOperationException.class)
	// Not yet supported
	public void testExecute_WhenFinderMethodIsFindingEntityList_WithSingleStringParameter_WithSort_WhenNotFindingByHashKey() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByNameOrderByNameAsc", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);

        // Mock out specific DynamoDBOperations behavior expected by this method
        // ArgumentCaptor<DynamoDBScanExpression> scanCaptor =
        // ArgumentCaptor.forClass(DynamoDBScanExpression.class);
        // ArgumentCaptor<Class<User>> classCaptor =
        // ArgumentCaptor.forClass(Class.class);
        // Mockito.when(mockUserScanResults.get(0)).thenReturn(mockUser);
        // Mockito.when(mockUserScanResults.size()).thenReturn(1);
        // Mockito.when(mockDynamoDBOperations.scan(classCaptor.capture(),
        // scanCaptor.capture())).thenReturn(
        // mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someName"};
        partTreeDynamoDBQuery.execute(parameters);

    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityList_WithSingleStringArrayParameter_WithIn_WhenNotFindingByHashKey() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByNameIn", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);

        String[] inParams = new String[]{"someName", "someOtherName"};

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{inParams};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected list of results
        assertEquals(o, mockUserScanResults);

        // Assert that the list of results contains the correct elements
        assertEquals(1, mockUserScanResults.size());
        assertEquals(mockUser, mockUserScanResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "name"), names);
        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(2, values.size());
        assertEquals(mapOf(
                ":value1", new AttributeValue().withS("someName"),
                ":value2", new AttributeValue().withS("someOtherName")), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 IN (:value1,:value2)"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityList_WithSingleListParameter_WithIn_WhenNotFindingByHashKey() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByNameIn", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);

        List<String> inParams = Arrays.asList("someName", "someOtherName");

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{inParams};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected list of results
        assertEquals(o, mockUserScanResults);

        // Assert that the list of results contains the correct elements
        assertEquals(1, mockUserScanResults.size());
        assertEquals(mockUser, mockUserScanResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "name"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(2, values.size());
        assertEquals(mapOf(
                ":value1", new AttributeValue().withS("someName"),
                ":value2", new AttributeValue().withS("someOtherName"))
                , values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 IN (:value1,:value2)"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityList_WithSingleDateParameter_WhenNotFindingByHashKey() throws ParseException {
        String joinDateString = "2013-09-12T14:04:03.123Z";
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date joinDate = dateFormat.parse(joinDateString);

        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByJoinDate", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{joinDate};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected list of results
        assertEquals(o, mockUserScanResults);

        // Assert that the list of results contains the correct elements
        assertEquals(1, mockUserScanResults.size());
        assertEquals(mockUser, mockUserScanResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "joinDate"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withS(joinDateString)), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 = :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityList_WithSingleDateParameter_WithCustomMarshaller_WhenNotFindingByHashKey() throws ParseException {
        String joinYearString = "2013";
        DateFormat dateFormat = new SimpleDateFormat("yyyy");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date joinYear = dateFormat.parse(joinYearString);

        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByJoinYear", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);
        DynamoDBYearMarshaller marshaller = new DynamoDBYearMarshaller();

        when(mockUserEntityMetadata.getMarshallerForProperty("joinYear")).thenReturn(marshaller);

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{joinYear};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected list of results
        assertEquals(o, mockUserScanResults);

        // Assert that the list of results contains the correct elements
        assertEquals(1, mockUserScanResults.size());
        assertEquals(mockUser, mockUserScanResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "joinYear"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withS(joinYearString)), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 = :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

    // Global Secondary Index Test 1
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityList_WithSingleDateParameter_WithCustomMarshaller_WhenFindingByGlobalSecondaryHashIndexHashKey() throws ParseException {
        String joinYearString = "2013";
        DateFormat dateFormat = new SimpleDateFormat("yyyy");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date joinYear = dateFormat.parse(joinYearString);

        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByJoinYear", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);
        DynamoDBYearMarshaller marshaller = new DynamoDBYearMarshaller();
        when(mockUserEntityMetadata.isGlobalIndexHashKeyProperty("joinYear")).thenReturn(true);

        when(mockUserEntityMetadata.getMarshallerForProperty("joinYear")).thenReturn(marshaller);

        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("joinYear", new String[]{"JoinYear-index"});
        when(mockUserEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockUserEntityMetadata.getDynamoDBTableName()).thenReturn("user");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockUserQueryResults.get(0)).thenReturn(mockUser);
        when(mockUserQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(userClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockUserQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(User.class, "user")).thenReturn("user");

        // Execute the query
        Object[] parameters = new Object[]{joinYear};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockUserQueryResults, o);
        assertEquals(1, mockUserQueryResults.size());
        assertEquals(mockUser, mockUserQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("JoinYear-index", indexName);

        assertEquals("user", queryResultCaptor.getValue().getTableName());

        // Assert that we have only one range condition for the global secondary index
        // hash key
        assertEquals(1, queryResultCaptor.getValue().getKeyConditions().size());
        Condition condition = queryResultCaptor.getValue().getKeyConditions().get("joinYear");
        assertEquals(ComparisonOperator.EQ.name(), condition.getComparisonOperator());
        assertEquals(1, condition.getAttributeValueList().size());
        assertEquals(joinYearString, condition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(condition.getAttributeValueList().get(0).getSS());
        assertNull(condition.getAttributeValueList().get(0).getN());
        assertNull(condition.getAttributeValueList().get(0).getNS());
        assertNull(condition.getAttributeValueList().get(0).getB());
        assertNull(condition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(userClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 2
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityList_WithDateParameterAndStringParameter_WithCustomMarshaller_WhenFindingByGlobalSecondaryHashAndRangeIndexHashAndRangeKey() throws ParseException {
        String joinYearString = "2013";
        DateFormat dateFormat = new SimpleDateFormat("yyyy");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date joinYear = dateFormat.parse(joinYearString);

        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByJoinYearAndPostCode", 2, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);
        DynamoDBYearMarshaller marshaller = new DynamoDBYearMarshaller();

        when(mockUserEntityMetadata.isGlobalIndexHashKeyProperty("joinYear")).thenReturn(true);
        when(mockUserEntityMetadata.isGlobalIndexRangeKeyProperty("postCode")).thenReturn(true);

        when(mockUserEntityMetadata.getMarshallerForProperty("joinYear")).thenReturn(marshaller);

        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("joinYear", new String[]{"JoinYear-index"});
        indexRangeKeySecondaryIndexNames.put("postCode", new String[]{"JoinYear-index"});

        when(mockUserEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockUserQueryResults.get(0)).thenReturn(mockUser);
        when(mockUserQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(userClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockUserQueryResults);
        when(mockUserEntityMetadata.getDynamoDBTableName()).thenReturn("user");
        when(mockDynamoDBOperations.getOverriddenTableName(User.class, "user")).thenReturn("user");

        // Execute the query
        Object[] parameters = new Object[]{joinYear, "nw1"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockUserQueryResults, o);
        assertEquals(1, mockUserQueryResults.size());
        assertEquals(mockUser, mockUserQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("JoinYear-index", indexName);

        // Assert that we have only two range conditions for the global secondary index
        // hash key and range key
        assertEquals(2, queryResultCaptor.getValue().getKeyConditions().size());
        Condition yearCondition = queryResultCaptor.getValue().getKeyConditions().get("joinYear");
        assertEquals(ComparisonOperator.EQ.name(), yearCondition.getComparisonOperator());
        assertEquals(1, yearCondition.getAttributeValueList().size());
        assertEquals(joinYearString, yearCondition.getAttributeValueList().get(0).getS());
        Condition postCodeCondition = queryResultCaptor.getValue().getKeyConditions().get("postCode");
        assertEquals(ComparisonOperator.EQ.name(), postCodeCondition.getComparisonOperator());
        assertEquals(1, postCodeCondition.getAttributeValueList().size());
        assertEquals("nw1", postCodeCondition.getAttributeValueList().get(0).getS());

        assertEquals("user", queryResultCaptor.getValue().getTableName());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(yearCondition.getAttributeValueList().get(0).getSS());
        assertNull(yearCondition.getAttributeValueList().get(0).getN());
        assertNull(yearCondition.getAttributeValueList().get(0).getNS());
        assertNull(yearCondition.getAttributeValueList().get(0).getB());
        assertNull(yearCondition.getAttributeValueList().get(0).getBS());
        assertNull(postCodeCondition.getAttributeValueList().get(0).getSS());
        assertNull(postCodeCondition.getAttributeValueList().get(0).getN());
        assertNull(postCodeCondition.getAttributeValueList().get(0).getNS());
        assertNull(postCodeCondition.getAttributeValueList().get(0).getB());
        assertNull(postCodeCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(userClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 3
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeKeyList_WhenFindingByGlobalSecondaryHashIndexHashKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByDisplayNameOrderByDisplayNameDesc", 1, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockPlaylistEntityMetadata.isGlobalIndexHashKeyProperty("displayName")).thenReturn(true);
        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("displayName", new String[]{"DisplayName-index"});
        when(mockPlaylistEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockPlaylistEntityMetadata.getDynamoDBTableName()).thenReturn("playlist");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(Playlist.class, "playlist")).thenReturn("playlist");

        // Execute the query
        Object[] parameters = new Object[]{"Michael"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("DisplayName-index", indexName);

        assertEquals("playlist", queryResultCaptor.getValue().getTableName());

        // Assert that we have only one range condition for the global secondary index
        // hash key
        assertEquals(1, queryResultCaptor.getValue().getKeyConditions().size());
        Condition condition = queryResultCaptor.getValue().getKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.EQ.name(), condition.getComparisonOperator());
        assertEquals(1, condition.getAttributeValueList().size());
        assertEquals("Michael", condition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(condition.getAttributeValueList().get(0).getSS());
        assertNull(condition.getAttributeValueList().get(0).getN());
        assertNull(condition.getAttributeValueList().get(0).getNS());
        assertNull(condition.getAttributeValueList().get(0).getB());
        assertNull(condition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 3a
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeKeyList_WhenFindingByGlobalSecondaryHashIndexHashKey_WhereSecondaryHashKeyIsPrimaryRangeKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistName", 1, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("playlistName", new String[]{"PlaylistName-index"});
        when(mockPlaylistEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockPlaylistEntityMetadata.getDynamoDBTableName()).thenReturn("playlist");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(Playlist.class, "playlist")).thenReturn("playlist");

        // Execute the query
        Object[] parameters = new Object[]{"Some Playlist"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("PlaylistName-index", indexName);

        assertEquals("playlist", queryResultCaptor.getValue().getTableName());

        // Assert that we have the correct conditions
        assertEquals(1, queryResultCaptor.getValue().getKeyConditions().size());
        Condition condition = queryResultCaptor.getValue().getKeyConditions().get("playlistName");
        assertEquals(ComparisonOperator.EQ.name(), condition.getComparisonOperator());
        assertEquals(1, condition.getAttributeValueList().size());
        assertEquals("Some Playlist", condition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(condition.getAttributeValueList().get(0).getSS());
        assertNull(condition.getAttributeValueList().get(0).getN());
        assertNull(condition.getAttributeValueList().get(0).getNS());
        assertNull(condition.getAttributeValueList().get(0).getB());
        assertNull(condition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 4
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeKeyList_WhenFindingByGlobalSecondaryHashAndRangeIndexHashAndRangeKey_WhereSecondaryHashKeyIsPrimaryHashKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByUserNameAndDisplayName", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockPlaylistEntityMetadata.isGlobalIndexHashKeyProperty("userName")).thenReturn(true);
        // Mockito.when(mockPlaylistEntityMetadata.isGlobalIndexRangeKeyProperty("displayName")).thenReturn(true);

        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("displayName", new String[]{"UserName-DisplayName-index"});
        indexRangeKeySecondaryIndexNames.put("userName", new String[]{"UserName-DisplayName-index"});

        when(mockPlaylistEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockPlaylistEntityMetadata.getDynamoDBTableName()).thenReturn("playlist");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(Playlist.class, "playlist")).thenReturn("playlist");

        // Execute the query
        Object[] parameters = new Object[]{"1", "Michael"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("UserName-DisplayName-index", indexName);

        assertEquals("playlist", queryResultCaptor.getValue().getTableName());

        // Assert that we the correct conditions
        assertEquals(2, queryResultCaptor.getValue().getKeyConditions().size());
        Condition globalRangeKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.EQ.name(), globalRangeKeyCondition.getComparisonOperator());
        assertEquals(1, globalRangeKeyCondition.getAttributeValueList().size());
        assertEquals("Michael", globalRangeKeyCondition.getAttributeValueList().get(0).getS());
        Condition globalHashKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("userName");
        assertEquals(ComparisonOperator.EQ.name(), globalHashKeyCondition.getComparisonOperator());
        assertEquals(1, globalHashKeyCondition.getAttributeValueList().size());
        assertEquals("1", globalHashKeyCondition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getBS());

        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 4b
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeKeyList_WhenFindingByGlobalSecondaryHashAndRangeIndexHashAndRangeKey_WhereSecondaryHashKeyIsPrimaryRangeKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistNameAndDisplayName", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockPlaylistEntityMetadata.isGlobalIndexHashKeyProperty("playlistName")).thenReturn(true);
        // Mockito.when(mockPlaylistEntityMetadata.isGlobalIndexRangeKeyProperty("displayName")).thenReturn(true);
        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("playlistName", new String[]{"PlaylistName-DisplayName-index"});
        indexRangeKeySecondaryIndexNames.put("displayName", new String[]{"PlaylistName-DisplayName-index"});

        when(mockPlaylistEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockPlaylistEntityMetadata.getDynamoDBTableName()).thenReturn("playlist");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(Playlist.class, "playlist")).thenReturn("playlist");

        // Execute the query
        Object[] parameters = new Object[]{"SomePlaylistName", "Michael"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("PlaylistName-DisplayName-index", indexName);

        assertEquals("playlist", queryResultCaptor.getValue().getTableName());

        // Assert that we have the correct conditions
        assertEquals(2, queryResultCaptor.getValue().getKeyConditions().size());
        Condition globalRangeKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.EQ.name(), globalRangeKeyCondition.getComparisonOperator());
        assertEquals(1, globalRangeKeyCondition.getAttributeValueList().size());
        assertEquals("Michael", globalRangeKeyCondition.getAttributeValueList().get(0).getS());
        Condition globalHashKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("playlistName");
        assertEquals(ComparisonOperator.EQ.name(), globalHashKeyCondition.getComparisonOperator());
        assertEquals(1, globalHashKeyCondition.getAttributeValueList().size());
        assertEquals("SomePlaylistName", globalHashKeyCondition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getBS());

        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 4c
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeKeyList_WhenFindingByGlobalSecondaryHashAndRangeIndexHashAndRangeKey_WhereSecondaryRangeKeyIsPrimaryRangeKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByDisplayNameAndPlaylistName", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockPlaylistEntityMetadata.isGlobalIndexHashKeyProperty("displayName")).thenReturn(true);
        // Mockito.when(mockPlaylistEntityMetadata.isGlobalIndexRangeKeyProperty("playlistName")).thenReturn(true);

        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("displayName", new String[]{"DisplayName-PlaylistName-index"});
        indexRangeKeySecondaryIndexNames.put("playlistName", new String[]{"DisplayName-PlaylistName-index"});

        when(mockPlaylistEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockPlaylistEntityMetadata.getDynamoDBTableName()).thenReturn("playlist");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(Playlist.class, "playlist")).thenReturn("playlist");

        // Execute the query
        Object[] parameters = new Object[]{"SomeDisplayName", "SomePlaylistName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("DisplayName-PlaylistName-index", indexName);

        assertEquals("playlist", queryResultCaptor.getValue().getTableName());

        // Assert that we have the correct conditions

        assertEquals(2, queryResultCaptor.getValue().getKeyConditions().size());
        Condition globalRangeKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.EQ.name(), globalRangeKeyCondition.getComparisonOperator());
        assertEquals(1, globalRangeKeyCondition.getAttributeValueList().size());
        assertEquals("SomeDisplayName", globalRangeKeyCondition.getAttributeValueList().get(0).getS());
        Condition globalHashKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("playlistName");
        assertEquals(ComparisonOperator.EQ.name(), globalHashKeyCondition.getComparisonOperator());
        assertEquals(1, globalHashKeyCondition.getAttributeValueList().size());
        assertEquals("SomePlaylistName", globalHashKeyCondition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getBS());

        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 4d
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeKeyList_WhenFindingByGlobalSecondaryHashAndRangeIndexHashAndRangeKey_WhereSecondaryRangeKeyIsPrimaryHashKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByDisplayNameAndUserName", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockPlaylistEntityMetadata.isGlobalIndexHashKeyProperty("displayName")).thenReturn(true);
        // Mockito.when(mockPlaylistEntityMetadata.isGlobalIndexRangeKeyProperty("userName")).thenReturn(true);

        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("displayName", new String[]{"DisplayName-UserName-index"});
        indexRangeKeySecondaryIndexNames.put("userName", new String[]{"DisplayName-UserName-index"});

        when(mockPlaylistEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockPlaylistEntityMetadata.getDynamoDBTableName()).thenReturn("playlist");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(Playlist.class, "playlist")).thenReturn("playlist");

        // Execute the query
        Object[] parameters = new Object[]{"SomeDisplayName", "SomeUserName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("DisplayName-UserName-index", indexName);

        assertEquals("playlist", queryResultCaptor.getValue().getTableName());

        // Assert that we have the correct conditions

        assertEquals(2, queryResultCaptor.getValue().getKeyConditions().size());
        Condition globalRangeKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.EQ.name(), globalRangeKeyCondition.getComparisonOperator());
        assertEquals(1, globalRangeKeyCondition.getAttributeValueList().size());
        assertEquals("SomeDisplayName", globalRangeKeyCondition.getAttributeValueList().get(0).getS());
        Condition globalHashKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("userName");
        assertEquals(ComparisonOperator.EQ.name(), globalHashKeyCondition.getComparisonOperator());
        assertEquals(1, globalHashKeyCondition.getAttributeValueList().size());
        assertEquals("SomeUserName", globalHashKeyCondition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getBS());

        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 4e
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeKeyList_WhenFindingByGlobalSecondaryHashAndRangeIndexHashAndRangeKeyNonEqualityCondition_WhereSecondaryHashKeyIsPrimaryHashKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByUserNameAndDisplayNameAfter", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockPlaylistEntityMetadata.isGlobalIndexHashKeyProperty("userName")).thenReturn(true);
        // Mockito.when(mockPlaylistEntityMetadata.isGlobalIndexRangeKeyProperty("displayName")).thenReturn(true);

        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("displayName", new String[]{"UserName-DisplayName-index"});
        indexRangeKeySecondaryIndexNames.put("userName", new String[]{"UserName-DisplayName-index"});

        when(mockPlaylistEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockPlaylistEntityMetadata.getDynamoDBTableName()).thenReturn("playlist");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(Playlist.class, "playlist")).thenReturn("playlist");

        // Execute the query
        Object[] parameters = new Object[]{"1", "Michael"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("UserName-DisplayName-index", indexName);

        assertEquals("playlist", queryResultCaptor.getValue().getTableName());

        // Assert that we the correct conditions
        assertEquals(2, queryResultCaptor.getValue().getKeyConditions().size());
        Condition globalRangeKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.GT.name(), globalRangeKeyCondition.getComparisonOperator());
        assertEquals(1, globalRangeKeyCondition.getAttributeValueList().size());
        assertEquals("Michael", globalRangeKeyCondition.getAttributeValueList().get(0).getS());
        Condition globalHashKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("userName");
        assertEquals(ComparisonOperator.EQ.name(), globalHashKeyCondition.getComparisonOperator());
        assertEquals(1, globalHashKeyCondition.getAttributeValueList().size());
        assertEquals("1", globalHashKeyCondition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getBS());

        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 4e2
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeKeyList_WhenFindingByGlobalSecondaryHashAndRangeIndexHashAndRangeKeyNonEqualityCondition_WhereSecondaryHashKeyIsPrimaryHashKey_WhenAccessingPropertyViaCompositeIdPath() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistIdUserNameAndDisplayNameAfter", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockPlaylistEntityMetadata.isGlobalIndexHashKeyProperty("userName")).thenReturn(true);
        // Mockito.when(mockPlaylistEntityMetadata.isGlobalIndexRangeKeyProperty("displayName")).thenReturn(true);

        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("displayName", new String[]{"UserName-DisplayName-index"});
        indexRangeKeySecondaryIndexNames.put("userName", new String[]{"UserName-DisplayName-index"});

        when(mockPlaylistEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockPlaylistEntityMetadata.getDynamoDBTableName()).thenReturn("playlist");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(Playlist.class, "playlist")).thenReturn("playlist");

        // Execute the query
        Object[] parameters = new Object[]{"1", "Michael"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("UserName-DisplayName-index", indexName);

        assertEquals("playlist", queryResultCaptor.getValue().getTableName());

        // Assert that we the correct conditions
        assertEquals(2, queryResultCaptor.getValue().getKeyConditions().size());
        Condition globalRangeKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.GT.name(), globalRangeKeyCondition.getComparisonOperator());
        assertEquals(1, globalRangeKeyCondition.getAttributeValueList().size());
        assertEquals("Michael", globalRangeKeyCondition.getAttributeValueList().get(0).getS());
        Condition globalHashKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("userName");
        assertEquals(ComparisonOperator.EQ.name(), globalHashKeyCondition.getComparisonOperator());
        assertEquals(1, globalHashKeyCondition.getAttributeValueList().size());
        assertEquals("1", globalHashKeyCondition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getBS());

        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 4f
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeKeyList_WhenFindingByGlobalSecondaryHashAndRangeIndexHashAndRangeKeyNonEqualityCondition_WhereSecondaryHashKeyIsPrimaryRangeKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByPlaylistNameAndDisplayNameAfter", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockPlaylistEntityMetadata.isGlobalIndexHashKeyProperty("playlistName")).thenReturn(true);
        // Mockito.when(mockPlaylistEntityMetadata.isGlobalIndexRangeKeyProperty("displayName")).thenReturn(true);
        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("playlistName", new String[]{"PlaylistName-DisplayName-index"});
        indexRangeKeySecondaryIndexNames.put("displayName", new String[]{"PlaylistName-DisplayName-index"});

        when(mockPlaylistEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockPlaylistEntityMetadata.getDynamoDBTableName()).thenReturn("playlist");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(Playlist.class, "playlist")).thenReturn("playlist");

        // Execute the query
        Object[] parameters = new Object[]{"SomePlaylistName", "Michael"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("PlaylistName-DisplayName-index", indexName);

        assertEquals("playlist", queryResultCaptor.getValue().getTableName());

        // Assert that we have the correct conditions
        assertEquals(2, queryResultCaptor.getValue().getKeyConditions().size());
        Condition globalRangeKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.GT.name(), globalRangeKeyCondition.getComparisonOperator());
        assertEquals(1, globalRangeKeyCondition.getAttributeValueList().size());
        assertEquals("Michael", globalRangeKeyCondition.getAttributeValueList().get(0).getS());
        Condition globalHashKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("playlistName");
        assertEquals(ComparisonOperator.EQ.name(), globalHashKeyCondition.getComparisonOperator());
        assertEquals(1, globalHashKeyCondition.getAttributeValueList().size());
        assertEquals("SomePlaylistName", globalHashKeyCondition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getBS());

        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 4g
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeKeyList_WhenFindingByGlobalSecondaryHashAndRangeIndexHashAndRangeKeyNonEqualityCondition_WhereSecondaryRangeKeyIsPrimaryRangeKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByDisplayNameAndPlaylistNameAfter", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockPlaylistEntityMetadata.isGlobalIndexHashKeyProperty("displayName")).thenReturn(true);
        when(mockPlaylistEntityMetadata.isGlobalIndexRangeKeyProperty("playlistName")).thenReturn(true);

        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("displayName", new String[]{"DisplayName-PlaylistName-index"});
        indexRangeKeySecondaryIndexNames.put("playlistName", new String[]{"DisplayName-PlaylistName-index"});

        when(mockPlaylistEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockPlaylistEntityMetadata.getDynamoDBTableName()).thenReturn("playlist");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(Playlist.class, "playlist")).thenReturn("playlist");

        // Execute the query
        Object[] parameters = new Object[]{"SomeDisplayName", "SomePlaylistName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("DisplayName-PlaylistName-index", indexName);

        assertEquals("playlist", queryResultCaptor.getValue().getTableName());

        // Assert that we have the correct conditions

        assertEquals(2, queryResultCaptor.getValue().getKeyConditions().size());
        Condition globalRangeKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.EQ.name(), globalRangeKeyCondition.getComparisonOperator());
        assertEquals(1, globalRangeKeyCondition.getAttributeValueList().size());
        assertEquals("SomeDisplayName", globalRangeKeyCondition.getAttributeValueList().get(0).getS());
        Condition globalHashKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("playlistName");
        assertEquals(ComparisonOperator.GT.name(), globalHashKeyCondition.getComparisonOperator());
        assertEquals(1, globalHashKeyCondition.getAttributeValueList().size());
        assertEquals("SomePlaylistName", globalHashKeyCondition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getBS());

        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 4h
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityWithCompositeKeyList_WhenFindingByGlobalSecondaryHashAndRangeIndexHashAndRangeKeyNonEqualityCondition_WhereSecondaryRangeKeyIsPrimaryHashKey() {

        setupCommonMocksForThisRepositoryMethod(mockPlaylistEntityMetadata, mockDynamoDBPlaylistQueryMethod,
                Playlist.class, "findByDisplayNameAndUserNameAfter", 2, "userName", "playlistName");
        when(mockDynamoDBPlaylistQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockPlaylistEntityMetadata.isGlobalIndexHashKeyProperty("displayName")).thenReturn(true);
        when(mockPlaylistEntityMetadata.isGlobalIndexRangeKeyProperty("userName")).thenReturn(true);

        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("displayName", new String[]{"DisplayName-UserName-index"});
        indexRangeKeySecondaryIndexNames.put("userName", new String[]{"DisplayName-UserName-index"});

        when(mockPlaylistEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockPlaylistEntityMetadata.getDynamoDBTableName()).thenReturn("playlist");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockPlaylistQueryResults.get(0)).thenReturn(mockPlaylist);
        when(mockPlaylistQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(playlistClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockPlaylistQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(Playlist.class, "playlist")).thenReturn("playlist");

        // Execute the query
        Object[] parameters = new Object[]{"SomeDisplayName", "SomeUserName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockPlaylistQueryResults, o);
        assertEquals(1, mockPlaylistQueryResults.size());
        assertEquals(mockPlaylist, mockPlaylistQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(playlistClassCaptor.getValue(), Playlist.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("DisplayName-UserName-index", indexName);

        assertEquals("playlist", queryResultCaptor.getValue().getTableName());

        // Assert that we have the correct conditions

        assertEquals(2, queryResultCaptor.getValue().getKeyConditions().size());
        Condition globalRangeKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("displayName");
        assertEquals(ComparisonOperator.EQ.name(), globalRangeKeyCondition.getComparisonOperator());
        assertEquals(1, globalRangeKeyCondition.getAttributeValueList().size());
        assertEquals("SomeDisplayName", globalRangeKeyCondition.getAttributeValueList().get(0).getS());
        Condition globalHashKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("userName");
        assertEquals(ComparisonOperator.GT.name(), globalHashKeyCondition.getComparisonOperator());
        assertEquals(1, globalHashKeyCondition.getAttributeValueList().size());
        assertEquals("SomeUserName", globalHashKeyCondition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getBS());

        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(playlistClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 4i
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityByGlobalSecondaryHashAndRangeIndexHashAndRangeKeyNonEqualityCondition_WhereBothSecondaryHashKeyAndSecondaryIndexRangeKeyMembersOfMultipleIndexes() {

        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByNameAndPostCodeAfter", 2, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockUserEntityMetadata.isGlobalIndexHashKeyProperty("name")).thenReturn(true);
        when(mockUserEntityMetadata.isGlobalIndexRangeKeyProperty("postCode")).thenReturn(true);

        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("name", new String[]{"Name-PostCode-index", "Name-JoinYear-index"});
        indexRangeKeySecondaryIndexNames.put("postCode", new String[]{"Name-PostCode-index", "Id-PostCode-index"});

        when(mockUserEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockUserEntityMetadata.getDynamoDBTableName()).thenReturn("user");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockUserQueryResults.get(0)).thenReturn(mockUser);
        when(mockUserQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(userClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockUserQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(User.class, "user")).thenReturn("user");

        // Execute the query
        Object[] parameters = new Object[]{"SomeName", "SomePostCode"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockUserQueryResults, o);
        assertEquals(1, mockUserQueryResults.size());
        assertEquals(mockUser, mockUserQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("Name-PostCode-index", indexName);

        assertEquals("user", queryResultCaptor.getValue().getTableName());

        // Assert that we have the correct conditions

        assertEquals(2, queryResultCaptor.getValue().getKeyConditions().size());
        Condition globalRangeKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("name");
        assertEquals(ComparisonOperator.EQ.name(), globalRangeKeyCondition.getComparisonOperator());
        assertEquals(1, globalRangeKeyCondition.getAttributeValueList().size());
        assertEquals("SomeName", globalRangeKeyCondition.getAttributeValueList().get(0).getS());
        Condition globalHashKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("postCode");
        assertEquals(ComparisonOperator.GT.name(), globalHashKeyCondition.getComparisonOperator());
        assertEquals(1, globalHashKeyCondition.getAttributeValueList().size());
        assertEquals("SomePostCode", globalHashKeyCondition.getAttributeValueList().get(0).getS());

        // Assert that all other attribute value types other than String type
        // are null
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalRangeKeyCondition.getAttributeValueList().get(0).getBS());

        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(userClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 4j
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityByGlobalSecondaryHashAndRangeIndexHashCondition_WhereSecondaryHashKeyMemberOfMultipleIndexes() {

        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByName", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockUserEntityMetadata.isGlobalIndexHashKeyProperty("name")).thenReturn(true);
        // Mockito.when(mockUserEntityMetadata.isGlobalIndexRangeKeyProperty("postCode")).thenReturn(true);
        // Mockito.when(mockUserEntityMetadata.isGlobalIndexRangeKeyProperty("joinYear")).thenReturn(true);
        // Mockito.when(mockUserEntityMetadata.isGlobalIndexHashKeyProperty("id")).thenReturn(true);

        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("name", new String[]{"Name-PostCode-index", "Name-JoinYear-index"});
        indexRangeKeySecondaryIndexNames.put("postCode", new String[]{"Name-PostCode-index", "Id-PostCode-index"});
        indexRangeKeySecondaryIndexNames.put("joinYear", new String[]{"Name-JoinYear-index"});
        indexRangeKeySecondaryIndexNames.put("id", new String[]{"Id-PostCode-index"});

        when(mockUserEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockUserEntityMetadata.getDynamoDBTableName()).thenReturn("user");

        // Mock out specific QueryRequestMapper behavior expected by this method
        ArgumentCaptor<QueryRequest> queryResultCaptor = ArgumentCaptor.forClass(QueryRequest.class);
        when(mockUserQueryResults.get(0)).thenReturn(mockUser);
        when(mockUserQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(userClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockUserQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(User.class, "user")).thenReturn("user");

        // Execute the query
        Object[] parameters = new Object[]{"SomeName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockUserQueryResults, o);
        assertEquals(1, mockUserQueryResults.size());
        assertEquals(mockUser, mockUserQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("Name-PostCode-index", indexName);

        assertEquals("user", queryResultCaptor.getValue().getTableName());

        // Assert that we have the correct conditions
        Condition globalHashKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("name");
        assertEquals(ComparisonOperator.EQ.name(), globalHashKeyCondition.getComparisonOperator());
        assertEquals(1, globalHashKeyCondition.getAttributeValueList().size());
        assertEquals("SomeName", globalHashKeyCondition.getAttributeValueList().get(0).getS());

        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(userClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    // Global Secondary Index Test 4k
    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityByGlobalSecondaryHashAndRangeIndexHashCondition_WhereSecondaryHashKeyMemberOfMultipleIndexes_WhereOneIndexIsExactMatch() {

        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByName", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockUserEntityMetadata.isGlobalIndexHashKeyProperty("name")).thenReturn(true);
        // Mockito.when(mockUserEntityMetadata.isGlobalIndexRangeKeyProperty("postCode")).thenReturn(true);
        // Mockito.when(mockUserEntityMetadata.isGlobalIndexRangeKeyProperty("joinYear")).thenReturn(true);
        // Mockito.when(mockUserEntityMetadata.isGlobalIndexHashKeyProperty("id")).thenReturn(true);

        Map<String, String[]> indexRangeKeySecondaryIndexNames = new HashMap<>();
        indexRangeKeySecondaryIndexNames.put("name",
                new String[]{"Name-PostCode-index", "Name-index", "Name-JoinYear-index"});
        indexRangeKeySecondaryIndexNames.put("postCode", new String[]{"Name-PostCode-index", "Id-PostCode-index"});
        indexRangeKeySecondaryIndexNames.put("joinYear", new String[]{"Name-JoinYear-index"});
        indexRangeKeySecondaryIndexNames.put("id", new String[]{"Id-PostCode-index"});

        when(mockUserEntityMetadata.getGlobalSecondaryIndexNamesByPropertyName())
                .thenReturn(indexRangeKeySecondaryIndexNames);

        when(mockUserEntityMetadata.getDynamoDBTableName()).thenReturn("user");

        // Mock out specific QueryRequestMapper behavior expected by this method
        when(mockUserQueryResults.get(0)).thenReturn(mockUser);
        when(mockUserQueryResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.query(userClassCaptor.capture(), queryResultCaptor.capture()))
                .thenReturn(mockUserQueryResults);
        when(mockDynamoDBOperations.getOverriddenTableName(User.class, "user")).thenReturn("user");

        // Execute the query
        Object[] parameters = new Object[]{"SomeName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected results
        assertEquals(mockUserQueryResults, o);
        assertEquals(1, mockUserQueryResults.size());
        assertEquals(mockUser, mockUserQueryResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        String indexName = queryResultCaptor.getValue().getIndexName();
        assertNotNull(indexName);
        assertEquals("Name-index", indexName);

        assertEquals("user", queryResultCaptor.getValue().getTableName());

        // Assert that we have the correct conditions
        Condition globalHashKeyCondition = queryResultCaptor.getValue().getKeyConditions().get("name");
        assertEquals(ComparisonOperator.EQ.name(), globalHashKeyCondition.getComparisonOperator());
        assertEquals(1, globalHashKeyCondition.getAttributeValueList().size());
        assertEquals("SomeName", globalHashKeyCondition.getAttributeValueList().get(0).getS());

        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getSS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getN());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getNS());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getB());
        assertNull(globalHashKeyCondition.getAttributeValueList().get(0).getBS());

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).query(userClassCaptor.getValue(), queryResultCaptor.getValue());
    }

    @Test
    public void testExecute_WhenFinderMethodIsFindingEntityList_WithSingleStringParameter_WithCustomMarshaller_WhenNotFindingByHashKey() {

        String postcode = "N1";

        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByPostCode", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);
        CaseChangingMarshaller marshaller = new CaseChangingMarshaller();

        when(mockUserEntityMetadata.getMarshallerForProperty("postCode")).thenReturn(marshaller);

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{postcode};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected list of results
        assertEquals(o, mockUserScanResults);

        // Assert that the list of results contains the correct elements
        assertEquals(1, mockUserScanResults.size());
        assertEquals(mockUser, mockUserScanResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "postCode"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withS("n1")), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 = :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityList_WithSingleIntegerParameter_WhenNotFindingByHashKey() {
        int numberOfPlaylists = 5;

        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByNumberOfPlaylists", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{numberOfPlaylists};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected list of results
        assertEquals(o, mockUserScanResults);

        // Assert that the list of results contains the correct elements
        assertEquals(1, mockUserScanResults.size());
        assertEquals(mockUser, mockUserScanResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "numberOfPlaylists"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withN("" + numberOfPlaylists)), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 = :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityList_WithSingleStringParameter_WhenNotFindingByNotHashKey() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByIdNot", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someId"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected list of results
        assertEquals(o, mockUserScanResults);

        // Assert that the list of results contains the correct elements
        assertEquals(1, mockUserScanResults.size());
        assertEquals(mockUser, mockUserScanResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have only one filter condition, for the name of the
        // property

        /*
         * TODO  This is the primary key of the entity. A search should be done over the index, not a scan
         */

        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "id"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withS("someId")), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 <> :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test
	public void testExecute_WhenFinderMethodIsFindingEntityList_WithSingleStringParameter_WhenNotFindingByNotAProperty() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "findByNameNot", 1, "id", null);
        when(mockDynamoDBUserQueryMethod.isCollectionQuery()).thenReturn(true);

        when(mockUserScanResults.get(0)).thenReturn(mockUser);
        when(mockUserScanResults.size()).thenReturn(1);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected list of results
        assertEquals(o, mockUserScanResults);

        // Assert that the list of results contains the correct elements
        assertEquals(1, mockUserScanResults.size());
        assertEquals(mockUser, mockUserScanResults.get(0));

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "name"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withS("someName")), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 <> :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test
	public void testExecute_WhenExistsQueryFindsNoEntity() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "existsByName", 1, "id", null);
        when(mockUserEntityMetadata.getOverriddenAttributeName("name")).thenReturn(Optional.of("Name"));

        // Mockito.when(mockUserScanResults.size()).thenReturn(0);
        when(mockUserScanResults.isEmpty()).thenReturn(true);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(false, o);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(User.class, userClassCaptor.getValue());

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "name"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withS("someName")), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 = :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test
	public void testExecute_WhenExistsQueryFindsOneEntity() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "existsByName", 1, "id", null);
        when(mockUserEntityMetadata.getOverriddenAttributeName("name")).thenReturn(Optional.of("Name"));

        // Mockito.when(mockUserScanResults.get(0)).thenReturn(mockUser);
        // Mockito.when(mockUserScanResults.size()).thenReturn(1);
        when(mockUserScanResults.isEmpty()).thenReturn(false);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(true, o);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "name"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withS("someName")), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 = :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test
	public void testExecute_WhenExistsQueryFindsMultipleEntities() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "existsByName", 1, "id", null);
        when(mockUserEntityMetadata.getOverriddenAttributeName("name")).thenReturn(Optional.of("Name"));

        // Mockito.when(mockUserScanResults.get(0)).thenReturn(mockUser);
        // Mockito.when(mockUserScanResults.get(1)).thenReturn(mockUser);
        // Mockito.when(mockUserScanResults.size()).thenReturn(2);
        when(mockUserScanResults.isEmpty()).thenReturn(false);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(true, o);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "name"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withS("someName")), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 = :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test
	public void testExecute_WhenExistsWithLimitQueryFindsNoEntity() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "existsTop1ByName", 1, "id", null);
        when(mockUserEntityMetadata.getOverriddenAttributeName("name")).thenReturn(Optional.of("Name"));

        // Mockito.when(mockUserScanResults.size()).thenReturn(0);
        when(mockUserScanResults.isEmpty()).thenReturn(true);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(false, o);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(User.class, userClassCaptor.getValue());

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();
        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "name"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withS("someName")), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 = :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }

	@Test
	public void testExecute_WhenExistsWithLimitQueryFindsOneEntity() {
        setupCommonMocksForThisRepositoryMethod(mockUserEntityMetadata, mockDynamoDBUserQueryMethod, User.class,
                "existsTop1ByName", 1, "id", null);
        when(mockUserEntityMetadata.getOverriddenAttributeName("name")).thenReturn(Optional.of("Name"));

        // Mockito.when(mockUserScanResults.get(0)).thenReturn(mockUser);
        // Mockito.when(mockUserScanResults.size()).thenReturn(1);
        when(mockUserScanResults.isEmpty()).thenReturn(false);
        when(mockDynamoDBOperations.scan(userClassCaptor.capture(), scanCaptor.capture()))
                .thenReturn(mockUserScanResults);

        // Execute the query
        Object[] parameters = new Object[]{"someName"};
        Object o = partTreeDynamoDBQuery.execute(parameters);

        // Assert that we obtain the expected single result
        assertEquals(true, o);

        // Assert that we scanned DynamoDB for the correct class
        assertEquals(userClassCaptor.getValue(), User.class);

        // Assert that we have only one filter condition, for the name of the
        // property
        DynamoDBScanExpression scanExpression = scanCaptor.getValue();
        Map<String, String> names = scanExpression.getExpressionAttributeNames();

        assertEquals(1, names.size());
        assertEquals(mapOf("#key1", "name"), names);

        Map<String, AttributeValue> values = scanExpression.getExpressionAttributeValues();
        assertEquals(1, values.size());
        assertEquals(mapOf(":value1", new AttributeValue().withS("someName")), values);

        assertThat(scanExpression.getFilterExpression(), containsString("#key1 = :value1"));

        // Verify that the expected DynamoDBOperations method was called
        Mockito.verify(mockDynamoDBOperations).scan(userClassCaptor.getValue(), scanCaptor.getValue());
    }
}
