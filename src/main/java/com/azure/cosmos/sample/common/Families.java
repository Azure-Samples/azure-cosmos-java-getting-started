// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.common;
import com.github.javafaker.Faker;

import java.util.UUID;

public class Families {
    private static final Faker faker = new Faker();
    
    public static Address addressPool[] = {
        Address.builder()
                .city("San Ramon")
                .county("King")
                .state("CA")
                .build(),

        Address.builder()
                .city("Mountain View")
                .state("CA")
                .county("Greene")
                .build(),

        Address.builder()
                .city("Austin")
                .state("TX")
                .county("Union")
                .build(),

        Address.builder()
                .city("Austin")
                .state("TX")
                .county("Monroe")
                .build(),

        Address.builder()
                .city("Seattle")
                .county("Jefferson")
                .state("WA")
                .build()

    };

    public static String gender[] = {"male", "female"};

    public static Pet generatePet()
    {
        return Pet.builder().givenName(faker.name().username()).build();
    }

    public static Child generateChild(String lastName)
    {
        int numberPets = faker.random().nextInt(4);
        Pet pets[] = new Pet[numberPets];
        for (int i=0; i<numberPets; i++)
        {
            pets[i] = generatePet();
        }
        return Child.builder()
                .firstName(faker.name().firstName())
                .familyName(lastName)
                .gender(gender[faker.random().nextInt(gender.length)])
                .grade(faker.random().nextInt(12))
                .pets(pets)
                .build();
    }
    public static Family generateFamily() {
        String lastName = faker.name().lastName();
        int numberChildren = faker.random().nextInt(4);
        Child children[] = new Child[numberChildren];
        for (int i=0; i<numberChildren; i++)
        {
            children[i] = generateChild(lastName);
        }
        return Family.builder()
                .address(addressPool[faker.random().nextInt(addressPool.length)])
                .lastName(lastName)
                .id(lastName + UUID.randomUUID().toString())
                .parents(new Parent[]{
                        Parent.builder()
                                .firstName(faker.name().firstName())
                                .familyName(lastName)
                                .build(),
                        Parent.builder()
                                .firstName(faker.name().firstName())
                                .familyName(lastName)
                                .build()
                })
                .children(children)
                .build();
    }
}
