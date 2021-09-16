// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.async;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.azure.cosmos.sample.common.AccountSettings;
import com.azure.cosmos.sample.common.Families;
import com.azure.cosmos.sample.common.Family;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collections;

public class AsyncMain {

    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    protected static Logger logger = LoggerFactory.getLogger(AsyncMain.class.getSimpleName());

    public void close() {
        client.close();
    }

    /**
     * Run a Hello CosmosDB console application.
     *
     * @param args command line args.
     */
    //  <Main>
    public static void main(String[] args) {
        AsyncMain p = new AsyncMain();

        try {
            logger.info("Starting ASYNC main");
            p.getStartedDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            logger.error("Cosmos getStarted failed with", e);
        } finally {
            logger.info("Closing the client");
            p.close();
        }
    }

    //  </Main>

    private void getStartedDemo() throws Exception {
        logger.info("Using Azure Cosmos DB endpoint: {}", AccountSettings.HOST);

        //  Create async client
        //  <CreateAsyncClient>
        client = new CosmosClientBuilder()
            .endpoint(AccountSettings.HOST)
            .key(AccountSettings.MASTER_KEY)
            //  Setting the preferred location to Cosmos DB Account region
            //  West US is just an example. User should set preferred location to the Cosmos DB region closest to the application
            .preferredRegions(Collections.singletonList("West US"))
            .consistencyLevel(ConsistencyLevel.EVENTUAL)
            //  Setting content response on write enabled, which enables the SDK to return response on write operations.
            .contentResponseOnWriteEnabled(true)
            .buildAsyncClient();

        //  </CreateAsyncClient>

        createDatabaseIfNotExists();
        createContainerIfNotExists();

             //  Setup family items to create
        Flux<Family> familiesToCreate = Flux.just(Families.generateFamily(),
                Families.generateFamily(),
                Families.generateFamily(),
                Families.generateFamily(),
                Families.generateFamily(),
                Families.generateFamily(),
                Families.generateFamily(),
                Families.generateFamily(),
                Families.generateFamily(),
                Families.generateFamily(),
                Families.generateFamily(),
                Families.generateFamily(),
                Families.generateFamily(),
                Families.generateFamily(),
                Families.generateFamily());


        createFamilies(familiesToCreate);

        logger.info("Reading items.");
      //  readItems(familiesToCreate);

        logger.info("Querying items.");
        queryItems();
    }

    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Create database {} if not exists.", databaseName);

        //  Create database if not exists
        //  <CreateDatabaseIfNotExists>
        Mono<CosmosDatabaseResponse> databaseResponseMono = client.createDatabaseIfNotExists(databaseName);
        databaseResponseMono.flatMap(databaseResponse -> {
            database = client.getDatabase(databaseResponse.getProperties().getId());
            logger.info("Checking database {} completed!\n", database.getId());
            return Mono.empty();
        }).block();
        //  </CreateDatabaseIfNotExists>
    }

    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container {} if not exists.", containerName);

        //  Create container if not exists
        //  <CreateContainerIfNotExists>

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/id");
        Mono<CosmosContainerResponse> containerResponseMono = database.createContainerIfNotExists(containerProperties, ThroughputProperties.createManualThroughput(400));

        //  Create container with 400 RU/s
        containerResponseMono.flatMap(containerResponse -> {
            container = database.getContainer(containerResponse.getProperties().getId());
            logger.info("Checking container {} completed!\n", container.getId());
            return Mono.empty();
        }).block();

        //  </CreateContainerIfNotExists>
    }

    private void createFamilies(Flux<Family> families) throws Exception {

        //  <CreateItem>

        try {

            //  Combine multiple item inserts, associated success println's, and a final aggregate stats println into one Reactive stream.
            double charge = families.flatMap(family -> {
                return container.createItem(family);
            }) //Flux of item request responses
                    .flatMap(itemResponse -> {
                        logger.info("Created item with request charge of {} within" +
                                        " duration {}",
                                itemResponse.getRequestCharge(), itemResponse.getDuration());
                        logger.info("Item ID: {}\n", itemResponse.getItem().getId());
                        return Mono.just(itemResponse.getRequestCharge());
                    }) //Flux of request charges
                    .reduce(0.0,
                            (charge_n, charge_nplus1) -> charge_n + charge_nplus1
                    ) //Mono of total charge - there will be only one item in this stream
                    .block(); //Preserve the total charge and print aggregate charge/item count stats.

            logger.info("Created items with total request charge of {}\n", charge);

        } catch (Exception err) {
            if (err instanceof CosmosException) {
                //Client-specific errors
                CosmosException cerr = (CosmosException) err;
                logger.error("Read Item failed with CosmosException\n", cerr);
            } else {
                //General errors
                logger.error("Read Item failed with error\n", err);
            }
        }

        //  </CreateItem>
    }

    private void readItems(Flux<Family> familiesToCreate) {
        //  Using partition key for point read scenarios.
        //  This will help fast look up of items because of partition key
        //  <ReadItem>

        try {

            familiesToCreate.flatMap(family -> {
                Mono<CosmosItemResponse<Family>> asyncItemResponseMono = container.readItem(family.getId(), new PartitionKey(family.getLastName()), Family.class);
                return asyncItemResponseMono;
            }).flatMap(itemResponse -> {
                double requestCharge = itemResponse.getRequestCharge();
                Duration requestLatency = itemResponse.getDuration();
                logger.info("Item successfully read with id {} with a charge of {} and within duration {}",
                        itemResponse.getItem().getId(), requestCharge, requestLatency);
                return Flux.empty();
            }).blockLast();

        } catch (Exception err) {
            if (err instanceof CosmosException) {
                //Client-specific errors
                CosmosException cerr = (CosmosException) err;
                logger.error("Read Item failed with CosmosException\n", cerr);
            } else {
                //General errors
                logger.error("Read Item failed\n", err);
            }
        }

        //  </ReadItem>
    }

    private void queryItems() {
        //  <QueryItems>
        // Set some common query options

        int preferredPageSize = 5; // We'll use this later

        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();

        //  Set populate query metrics to get metrics around query executions
        queryOptions.setQueryMetricsEnabled(true);

//        CosmosPagedFlux<Family> pagedFluxResponse = container.queryItems(
//                "SELECT * FROM Family WHERE Family.lastName IN ('Andersen', 'Wakefield', 'Johnson')", queryOptions, Family.class);

        try {
        CosmosPagedFlux<JsonNode> pagedFluxResponse = container.queryItems(
                "SELECT VALUE root FROM (SELECT DISTINCT i FROM i JOIN (SELECT DISTINCT VALUE bla FROM bla IN i.children WHERE bla.gender='male') where i.district=null) as root ORDER BY root.i._ts DESC"
                       , queryOptions, JsonNode.class);



            pagedFluxResponse.byPage(preferredPageSize)
                    .flatMap(fluxResponse -> {
                logger.info("Got a page of query result with " +
                        fluxResponse.getResults().size() + " items(s)"
                        + " and request charge of " + fluxResponse.getRequestCharge());

                return Flux.empty();
            }).blockFirst();

        } catch(Exception err) {
            if (err instanceof CosmosException) {
                //Client-specific errors
                CosmosException cerr = (CosmosException) err;
                logger.error("Read Item failed with CosmosException\n", cerr);
            } else {
                //General errors
                logger.error("Read Item failed\n", err);
            }
        }

        // </QueryItems>
    }
}
