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
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.sample.common.AccountSettings;
import com.azure.cosmos.sample.common.Families;
import com.azure.cosmos.sample.common.Family;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.Collectors;

public class AsyncPasswordlessMain {

    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    protected static Logger logger = LoggerFactory.getLogger(AsyncPasswordlessMain.class.getSimpleName());

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
        AsyncPasswordlessMain p = new AsyncPasswordlessMain();

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
        //  <CreatePasswordlessAsyncClient>
        DefaultAzureCredential credential = new DefaultAzureCredentialBuilder().build();

        client = new CosmosClientBuilder()
            .endpoint(AccountSettings.HOST)
            .credential(credential)
            //  Setting the preferred location to Cosmos DB Account region
            //  West US is just an example. User should set preferred location to the Cosmos DB region closest to the application
            .preferredRegions(Collections.singletonList("West US"))
            .consistencyLevel(ConsistencyLevel.EVENTUAL)
            //  Setting content response on write enabled, which enables the SDK to return response on write operations.
            .contentResponseOnWriteEnabled(true)
            .buildAsyncClient();

        //  </CreatePasswordlessAsyncClient>

        //database and container creation can only be done via control plane
        //ensure you have created a database named AzureSampleFamilyDB
        database = client.getDatabase(databaseName);
        //ensure you have created the container named FamilyContainer, partitioned by /lastName
        container = database.getContainer(containerName);

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

        logger.info("Reading items.");
        readItems(familiesToCreate);

        logger.info("Querying items.");
        queryItems();
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
                logger.error("Read Item failed with CosmosException\n", cerr);
            } else {
                //General errors
                logger.error("Read Item failed\n", err);
            }
        }

        // </QueryItems>
    }
}
