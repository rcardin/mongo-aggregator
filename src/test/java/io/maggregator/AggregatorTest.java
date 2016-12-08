/*
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2015 Riccardo Cardin
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * <p>
 *
 * @author Riccardo Cardin
 * @version 1.0
 * @since 1.0
 */
package io.maggregator;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.BsonField;
import io.maggregator.util.EmbeddedMongo;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit test of class {@link Aggregator}.
 *
 * @author Riccardo Cardin
 * @version 1.0
 * @since 1.0
 */
public class AggregatorTest {

    private static final String MISSING_DATABASE = "MongoDatabase cannot be null";
    private static final String MISSING_PARAM = "Aggregation cannot be performed due to the " +
            "lack of some input parameters";

    private static final String DATABASE = "test";
    private static final String COLLECTION = "collection";

    private static EmbeddedMongo embeddedMongo;

    @BeforeClass
    public static void setUp() throws Exception {
        embeddedMongo = new EmbeddedMongo();
        embeddedMongo.importFile(DATABASE, COLLECTION, "pipeline-test.json");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        embeddedMongo.close();
    }

    @Test
    public void executeShouldThrowAnExceptionIfDatabaseIsNotSet() {
        try {
            Aggregator.of(null)
                    .execute(MongoIterable::first);
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage()).isEqualTo(MISSING_DATABASE);
        }
    }

    @Test
    public void executeShouldThrowAnExceptionIfCollectionIsNotSet() {
        try {
            Aggregator.of(mock(MongoDatabase.class))
                    .filter(new BasicDBObject())
                    .projection(new BasicDBObject())
                    .groupBy(new BasicDBObject())
                    .execute(MongoIterable::first);
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage()).isEqualTo(MISSING_PARAM);
        }
    }

    @Test
    public void executeShouldThrowAnExceptionIfFiltersAreNotSet() {
        try {
            Aggregator.of(mock(MongoDatabase.class))
                    .collection("collection")
                    .projection(new BasicDBObject())
                    .groupBy(new BasicDBObject())
                    .execute(MongoIterable::first);
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage()).isEqualTo(MISSING_PARAM);
        }
    }

    @Test
    public void executeShouldThrowAnExceptionIfProjectionsAreNotSet() {
        try {
            Aggregator.of(mock(MongoDatabase.class))
                    .collection("collection")
                    .filter(new BasicDBObject())
                    .groupBy(new BasicDBObject())
                    .execute(MongoIterable::first);
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage()).isEqualTo(MISSING_PARAM);
        }
    }

    @Test
    public void executeShouldThrowAnExceptionIfGroupByIsNotSet() {
        try {
            Aggregator.of(mock(MongoDatabase.class))
                    .collection("collection")
                    .filter(new BasicDBObject())
                    .projection(new BasicDBObject())
                    .execute(MongoIterable::first);
        }    catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage()).isEqualTo(MISSING_PARAM);
        }
    }

    @Test
    public void executeShouldFilterProperly() throws Exception {
        List<Document> result =
                Aggregator
                        .of(embeddedMongo.mongoClient().getDatabase(DATABASE))
                        .collection(COLLECTION)
                        .filter(eq("name", "MongoDB"))
                        .projection(project(fields(excludeId(), include("name", "count"))))
                        .groupBy(group("$name", new BsonField("total", new BasicDBObject("$sum", "$count"))))
                        .execute(documents -> {
                            List<Document> list = new ArrayList<>();
                            for (Document doc: documents) {
                                list.add(doc);
                            }
                            return list;
                        });
        assertThat(result).hasSize(1);
        // Check that the filter operation has thrown away all documents with name
        // different from 'MongoDB'
        assertThat(result.get(0)).containsEntry("_id", "MongoDB");
    }

    @Test
    public void executeShouldProjectProperly() throws Exception {
        List<Document> result =
                Aggregator
                        .of(embeddedMongo.mongoClient().getDatabase(DATABASE))
                        .collection(COLLECTION)
                        .filter(eq("name", "MongoDB"))
                        .projection(project(fields(excludeId(), include("count"), computed("coords", "$info.x"))))
                        .groupBy(group("$coords", new BsonField("total", new BasicDBObject("$sum", "$count"))))
                        .execute(documents -> {
                            List<Document> list = new ArrayList<>();
                            for (Document doc: documents) {
                                list.add(doc);
                            }
                            return list;
                        });
        assertThat(result).hasSize(1);
        // Check that the projection has generated a new field 'coords'
        assertThat(result.get(0)).containsEntry("_id", 203);
    }

    @Test
    public void executeShouldGroupProperly() throws Exception {
        List<Document> result =
                Aggregator
                .of(embeddedMongo.mongoClient().getDatabase(DATABASE))
                .collection(COLLECTION)
                .filter(eq("type", "database"))
                .projection(project(fields(excludeId(), include("type", "count"))))
                .groupBy(group("$type", new BsonField("total", new BasicDBObject("$sum", "$count"))))
                .execute(documents -> {
                    List<Document> list = new ArrayList<>();
                    for (Document doc : documents) {
                        list.add(doc);
                    }
                    return list;
                });
        assertThat(result).hasSize(1);
        // Check that the group ha created a unique document that sums the 'count' field
        assertThat(result.get(0)).containsEntry("total", 6);
    }
}
