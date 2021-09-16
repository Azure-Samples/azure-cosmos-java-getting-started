// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.common;
import lombok.Builder;

@Builder
public class Child {
    private String familyName;
    private String firstName;
    private String gender;
    private int grade;
    private Pet[] pets;

    public int getGrade() {
        return grade;
    }

    public String getFamilyName() {
        return familyName;
    }

    public String getFirstName() {
        return firstName;
    }

    public Pet[] getPets() {
        return pets;
    }

    public String getGender() {
        return gender;
    }
}

