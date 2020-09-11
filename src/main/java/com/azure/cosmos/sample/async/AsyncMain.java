// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.async;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.sample.common.AccountSettings;
import com.azure.cosmos.sample.common.Families;
import com.azure.cosmos.sample.common.Family;
import com.azure.cosmos.util.CosmosPagedFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.Collectors;

public class AsyncMain {

    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

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
            System.out.println("Starting ASYNC main");
            p.getStartedDemo();
            System.out.println("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            System.out.println("Closing the client");
            p.close();
        }
        System.exit(0);
    }

    //  </Main>

    private void getStartedDemo() throws Exception {
        System.out.println("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

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

        Family andersenFamilyItem=Families.getAndersenFamilyItem();
        Family wakefieldFamilyItem=Families.getWakefieldFamilyItem();
        Family johnsonFamilyItem=Families.getJohnsonFamilyItem();
        Family smithFamilyItem=Families.getSmithFamilyItem();

        //  Setup family items to create
        Flux<Family> familiesToCreate = Flux.just(andersenFamilyItem,
                                            wakefieldFamilyItem,
                                            johnsonFamilyItem,
                                            smithFamilyItem);

        createFamilies(familiesToCreate);

        familiesToCreate = Flux.just(andersenFamilyItem,
                                wakefieldFamilyItem,
                                johnsonFamilyItem,
                                smithFamilyItem);

        System.out.println("Reading items.");
        readItems(familiesToCreate);

        System.out.println("Querying items.");
        queryItems();
    }

    private void createDatabaseIfNotExists() throws Exception {
        System.out.println("Create database " + databaseName + " if not exists.");

        //  Create database if not exists
        //  <CreateDatabaseIfNotExists>
        Mono<CosmosDatabaseResponse> databaseResponseMono = client.createDatabaseIfNotExists(databaseName);
        databaseResponseMono.flatMap(databaseResponse -> {
            database = client.getDatabase(databaseResponse.getProperties().getId());
            System.out.println("Checking database " + database.getId() + " completed!\n");
            return Mono.empty();
        }).block();
        //  </CreateDatabaseIfNotExists>
    }

    private void createContainerIfNotExists() throws Exception {
        System.out.println("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        //  <CreateContainerIfNotExists>

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/lastName");
        Mono<CosmosContainerResponse> containerResponseMono = database.createContainerIfNotExists(containerProperties, ThroughputProperties.createManualThroughput(400));
        
        //  Create container with 400 RU/s
        containerResponseMono.flatMap(containerResponse -> {
            container = database.getContainer(containerResponse.getProperties().getId());
            System.out.println("Checking container " + container.getId() + " completed!\n");
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
                        logger.info(String.format("Created item with request charge of %.2f within" +
                                        " duration %s",
                                itemResponse.getRequestCharge(), itemResponse.getDuration()));
                        logger.info(String.format("Item ID: %s\n", itemResponse.getItem().getId()));
                        return Mono.just(itemResponse.getRequestCharge());
                    }) //Flux of request charges
                    .reduce(0.0,
                            (charge_n, charge_nplus1) -> charge_n + charge_nplus1
                    ) //Mono of total charge - there will be only one item in this stream
                    .block(); //Preserve the total charge and print aggregate charge/item count stats.

            logger.info(String.format("Created items with total request charge of %.2f\n", charge));

        } catch (Exception err) {
            if (err instanceof CosmosException) {
                //Client-specific errors
                CosmosException cerr = (CosmosException) err;
                cerr.printStackTrace();
                logger.error(String.format("Read Item failed with %s\n", cerr));
            } else {
                //General errors
                err.printStackTrace();
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
                logger.info(String.format("Item successfully read with id %s with a charge of %.2f and within duration %s",
                        itemResponse.getItem().getId(), requestCharge, requestLatency));
                return Flux.empty();
            }).blockLast();

        } catch (Exception err) {
            if (err instanceof CosmosException) {
                //Client-specific errors
                CosmosException cerr = (CosmosException) err;
                cerr.printStackTrace();
                logger.error(String.format("Read Item failed with %s\n", cerr));
            } else {
                //General errors
                err.printStackTrace();
            }
        }

        //  </ReadItem>
    }

    private void queryItems() {
        //  <QueryItems>
        // Set some common query options

        int preferredPageSize = 10; // We'll use this later

        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();

        //  Set populate query metrics to get metrics around query executions
        queryOptions.setQueryMetricsEnabled(true);

        CosmosPagedFlux<Family> pagedFluxResponse = container.queryItems(
                "SELECT * FROM Family WHERE Family.lastName IN ('Andersen', 'Wakefield', 'Johnson')", queryOptions, Family.class);

        try {

            pagedFluxResponse.byPage(preferredPageSize).flatMap(fluxResponse -> {
                logger.info("Got a page of query result with " +
                        fluxResponse.getResults().size() + " items(s)"
                        + " and request charge of " + fluxResponse.getRequestCharge());

                logger.info("Item Ids " + fluxResponse
                        .getResults()
                        .stream()
                        .map(Family::getId)
                        .collect(Collectors.toList()));

                return Flux.empty();
            }).blockLast();

        } catch(Exception err) {
            if (err instanceof CosmosException) {
                //Client-specific errors
                CosmosException cerr = (CosmosException) err;
                cerr.printStackTrace();
                logger.error(String.format("Read Item failed with %s\n", cerr));
            } else {
                //General errors
                err.printStackTrace();
            }
        }

        // </QueryItems>
    }
}
