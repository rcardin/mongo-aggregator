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
package io.maggregator.util;

import com.mongodb.MongoClient;
import de.flapdoodle.embed.mongo.*;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.extract.UserTempNaming;
import de.flapdoodle.embed.process.runtime.Network;

import java.io.IOException;

/**
 * Please, insert description here.
 *
 * @author Riccardo Cardin
 * @version 1.0
 * @since 1.0
 */
public class EmbeddedMongo implements AutoCloseable {
    private MongodStarter starter;

    private MongodExecutable mongodExe;
    private MongodProcess mongod;

    private MongoClient mongo;

    private IMongodConfig mongodConfig;

    public EmbeddedMongo() throws IOException {
        Command command = Command.MongoD;
        int port = Network.getFreeServerPort();
        IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                .defaults(command)
                .artifactStore(new ExtractedArtifactStoreBuilder()
                        .defaults(command)
                        .download(new DownloadConfigBuilder()
                                .defaultsForCommand(command).build())
                        .executableNaming(new UserTempNaming()))
                .build();

        mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(port, Network.localhostIsIPv6()))
                .build();

        starter = MongodStarter.getInstance(runtimeConfig);

        mongodExe = starter.prepare(new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(port, Network.localhostIsIPv6()))
                .build());
        mongod = mongodExe.start();
        mongo = new MongoClient("localhost", port);
    }

    @Override
    public void close() throws Exception {
        mongod.stop();
        mongodExe.stop();
    }

    /**
     * Imports file {@code jsonFile} into {@code collection} of database {@code database}.
     * The file must represent an
     * array of Json documents. The collection will be drop before the insertion.
     *
     * @param dbName Target database name
     * @param collection Target collection
     * @param jsonFile Path of the json file.
     * @throws IOException
     */
    public void importFile(String dbName, String collection, String jsonFile) throws IOException {
        startMongoImport(dbName, collection, jsonFile, true, true, true);
    }

    private MongoImportProcess startMongoImport(String dbName, String collection, String jsonFile,
                                                Boolean jsonArray, Boolean upsert, Boolean drop)
            throws IOException {

        String resolvedJsonFile =
                Thread.currentThread().getContextClassLoader().getResource(jsonFile).toString();
        resolvedJsonFile=resolvedJsonFile.replaceFirst("file:",""); //.substring(1);

        IMongoImportConfig mongoImportConfig = new MongoImportConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(mongodConfig.net().getPort(), Network.localhostIsIPv6()))
                .db(dbName)
                .collection(collection)
                .upsert(upsert)
                .dropCollection(drop)
                .jsonArray(jsonArray)
                .importFile(resolvedJsonFile)
                .build();

        MongoImportExecutable mongoImportExecutable = MongoImportStarter.getDefaultInstance().prepare(mongoImportConfig);
        MongoImportProcess mongoImport = mongoImportExecutable.start();
        return mongoImport;
    }

    public MongoClient mongoClient() {
        return mongo;
    }

}
