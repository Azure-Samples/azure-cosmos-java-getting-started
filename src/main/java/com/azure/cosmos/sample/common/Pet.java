// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.common;
import lombok.Builder;

@Builder
public class Pet {
    private String givenName;

    public String getGivenName() {
        return givenName;
    }
}
