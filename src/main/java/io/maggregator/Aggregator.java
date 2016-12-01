/**
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
 * Please, insert description here.
 *
 * @author Riccardo Cardin
 * @version 1.0
 * @since 1.0
 */

/**
 * Please, insert description here.
 *
 * @author Riccardo Cardin
 * @version 1.0
 * @since 1.0
 */
package io.maggregator;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static com.mongodb.client.model.Aggregates.match;

/**
 * Fluent API for MongoDB Aggregation Framework.
 *
 * @author Riccardo Cardin
 * @version 1.0
 * @since 1.0
 */
public class Aggregator {
    private MongoDatabase mongoDatabase;

    private String collection;
    private Bson filter;
    private Bson projection;
    private Bson groupBy;

    private Aggregator(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }

    /**
     * Returns a new aggregator
     *
     * @param mongoDatabase Connection to the database
     * @return The aggregator
     *
     * @throws IllegalArgumentException if {@code mongoDatabase} is null
     */
    public static final Aggregator of(MongoDatabase mongoDatabase) {
        if (mongoDatabase == null)
            throw new IllegalArgumentException("MongoDatabase cannot be null");
        return new Aggregator(mongoDatabase);
    }

    /**
     * Sets the name of the collection to aggregate.
     *
     * @param name
     * @return A reference to this aggregator
     */
    public Aggregator collection(String name) {
        this.collection = name;
        return this;
    }

    /**
     * Filter to apply to the collection.
     *
     * @param filter
     * @return A reference to this aggregator
     */
    public Aggregator filter(Bson filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Properties to retain after the filtering phase.
     *
     * @param projection A document contaning the propeties
     * @return A reference to this aggregator
     */
    public Aggregator projection(Bson projection) {
        this.projection = projection;
        return this;
    }

    /**
     * Properties on which aggregate.
     *
     * @param groupBy A document contaning the propeties
     * @return A reference to this aggregator
     */
    public Aggregator groupBy(Bson groupBy) {
        this.groupBy = groupBy;
        return this;
    }

    /**
     * Applies the aggregation to the collection and uses {@code transformation} to get the
     * information in a usable format.
     *
     * @param transformation The function that transform the result of the aggregation process to
     *                       a usable format
     * @param <T> Return type
     * @return The transformed result of the aggregation process
     * @throws IllegalArgumentException If the collection or the filters or the projection fields
     *                                  or the group by clause or the transformation are not present
     */
    public <T> T execute(Function<AggregateIterable<Document>, T> transformation) {

        validate(transformation);

        MongoCollection collection = mongoDatabase.getCollection(this.collection);

        List<Bson> pipeline = Arrays.asList(match(filter), projection, groupBy);

        final AggregateIterable aggregate = collection.aggregate(pipeline);

        return (T) transformation.apply(aggregate);
    }

    private <T> void validate(Function<AggregateIterable<Document>, T> transformation) throws IllegalArgumentException {
        if (collection == null || filter == null || projection == null || groupBy == null ||
                transformation == null) {
            throw new IllegalArgumentException("Aggregation cannot be performed due to the " +
                    "lack of some input parameters");
        }
    }

    /**
     * Returns the result of the {@code aggregate} extracting the value of the {@code propertyName},
     * or {@code identity} if nothing was aggregated.
     *
     * @param aggregate Result of an aggregation process
     * @param propertyName Name of the property by which taking the value to be returned
     * @param identity Value returned if the aggregation is empty
     * @param <T> Return type
     *
     * @return Value associated to {@code propertyName} in the document built during the
     *         aggregation process
     */
    public static <T> T count(AggregateIterable<Document> aggregate,
                              String propertyName, T identity) {
        Iterator iterator = aggregate.iterator();
        if (!iterator.hasNext()) {
            return identity;
        }
        Document result = (Document) iterator.next();
        return (T) result.get(propertyName);
    }
}
