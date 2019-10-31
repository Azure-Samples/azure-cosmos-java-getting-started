// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.sync;

import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosContainerProperties;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosItem;
import com.azure.cosmos.CosmosItemProperties;
import com.azure.cosmos.CosmosItemRequestOptions;
import com.azure.cosmos.CosmosItemResponse;
import com.azure.cosmos.FeedOptions;
import com.azure.cosmos.FeedResponse;
import com.azure.cosmos.Resource;
import com.azure.cosmos.sample.common.AccountSettings;
import com.azure.cosmos.sample.common.Families;
import com.azure.cosmos.sample.common.Family;
import com.google.common.collect.Lists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SyncMain {

    private CosmosClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosDatabase database;
    private CosmosContainer container;

    public void close() {
        client.close();
    }

    /**
     * Run a Hello CosmosDB console application.
     *
     * @param args command line args.
     */
    //  <MainRegion>
    public static void main(String[] args) {
        SyncMain p = new SyncMain();

        try {
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

    //  </MainRegion>

    private void getStartedDemo() throws Exception {
        System.out.println("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ConnectionPolicy defaultPolicy = ConnectionPolicy.getDefaultPolicy();
        //  Setting the preferred location to Cosmos DB Account region
        //  West US is just an example. User should set preferred location to the Cosmos DB region closest to the application
        defaultPolicy.setPreferredLocations(Lists.newArrayList("West US"));

        //  Create sync client
        //  <CreateSyncClientRegion>
        client = new CosmosClientBuilder()
            .setEndpoint(AccountSettings.HOST)
            .setKey(AccountSettings.MASTER_KEY)
            .setConnectionPolicy(defaultPolicy)
            .setConsistencyLevel(ConsistencyLevel.EVENTUAL)
            .buildClient();

        //  </CreateSyncClientRegion>

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        //  Setup family items to create
        ArrayList<Family> familiesToCreate = new ArrayList<>();
        familiesToCreate.add(Families.getAndersenFamilyItem());
        familiesToCreate.add(Families.getWakefieldFamilyItem());
        familiesToCreate.add(Families.getJohnsonFamilyItem());
        familiesToCreate.add(Families.getSmithFamilyItem());

        createFamilies(familiesToCreate);

        System.out.println("Reading items.");
        readItems(familiesToCreate);

        System.out.println("Querying items.");
        queryItems();
    }

    private void createDatabaseIfNotExists() throws Exception {
        System.out.println("Create database " + databaseName + " if not exists.");

        //  Create database if not exists
        //  <CreateDatabaseIfNotExistsRegion>
        database = client.createDatabaseIfNotExists(databaseName).getDatabase();
        //  </CreateDatabaseIfNotExistsRegion>

        System.out.println("Checking database " + database.getId() + " completed!\n");
    }

    private void createContainerIfNotExists() throws Exception {
        System.out.println("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        //  <CreateContainerIfNotExists>
        CosmosContainerProperties containerProperties =
            new CosmosContainerProperties(containerName, "/lastName");

        //  Create container with 400 RU/s
        container = database.createContainerIfNotExists(containerProperties, 400).getContainer();
        //  </CreateContainerIfNotExists>

        System.out.println("Checking container " + container.getId() + " completed!\n");
    }

    private void createFamilies(List<Family> families) throws Exception {
        double totalRequestCharge = 0;
        for (Family family : families) {

            //  Create item using container that we created using sync client

            //  Use lastName as partitionKey for cosmos item
            //  Using appropriate partition key improves the performance of database operations
            CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions(family.getLastName());
            CosmosItemResponse item = container.createItem(family, cosmosItemRequestOptions);

            //  Get request charge and other properties like latency, and diagnostics strings, etc.
            System.out.println(String.format("Created item with request charge of %.2f within" +
                    " duration %s",
                item.getRequestCharge(), item.getRequestLatency()));
            totalRequestCharge += item.getRequestCharge();
        }
        System.out.println(String.format("Created %d items with total request " +
                "charge of %.2f",
            families.size(),
            totalRequestCharge));
    }

    private void readItems(ArrayList<Family> familiesToCreate) {
        //  Using partition key for point read scenarios.
        //  This will help fast look up of items because of partition key
        familiesToCreate.forEach(family -> {
            CosmosItem item = container.getItem(family.getId(), family.getLastName());
            try {
                CosmosItemResponse read = item.read(new CosmosItemRequestOptions(family.getLastName()));
                double requestCharge = read.getRequestCharge();
                Duration requestLatency = read.getRequestLatency();
                System.out.println(String.format("Item successfully read with id %s with a charge of %.2f and within duration %s",
                    read.getItem().getId(), requestCharge, requestLatency));
            } catch (CosmosClientException e) {
                e.printStackTrace();
                System.err.println(String.format("Read Item failed with %s", e));
            }
        });
    }

    private void queryItems() {
        // Set some common query options
        FeedOptions queryOptions = new FeedOptions();
        queryOptions.maxItemCount(10);
        queryOptions.setEnableCrossPartitionQuery(true);
        //  Set populate query metrics to get metrics around query executions
        queryOptions.populateQueryMetrics(true);

        Iterator<FeedResponse<CosmosItemProperties>> feedResponseIterator = container.queryItems(
            "SELECT * FROM Family WHERE Family.lastName IN ('Andersen', 'Wakefield', 'Johnson')", queryOptions);

        feedResponseIterator.forEachRemaining(cosmosItemPropertiesFeedResponse -> {
            System.out.println("Got a page of query result with " +
                cosmosItemPropertiesFeedResponse.getResults().size() + " items(s)"
                + " and request charge of " + cosmosItemPropertiesFeedResponse.getRequestCharge());

            System.out.println("Item Ids " + cosmosItemPropertiesFeedResponse
                .getResults()
                .stream()
                .map(Resource::getId)
                .collect(Collectors.toList()));
        });
    }
}
