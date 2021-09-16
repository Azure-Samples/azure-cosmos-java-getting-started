// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.common;
import lombok.Builder;

@Builder
public class Parent {
    private String familyName;
    private String firstName;

    public String getFirstName() {
        return firstName;
    }

    public String getFamilyName() {
        return familyName;
    }
}
