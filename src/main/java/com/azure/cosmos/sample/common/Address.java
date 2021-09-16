// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.common;
import lombok.Builder;

@Builder
public class Address {
    private String state;
    private String county;
    private String city;

    public String getCity() {
        return city;
    }

    public String getCounty() {
        return county;
    }

    public void setState(String state) {
        this.state = state;
    }
}

