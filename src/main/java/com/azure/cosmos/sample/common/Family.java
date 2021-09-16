// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.common;

import lombok.Builder;

@Builder
public class Family {
    private String id;
    private String lastName;
    private String district;
    private Parent[] parents;
    private Child[] children;
    private Address address;
    private boolean isRegistered;

    public Parent[] getParents()
    {
        return this.parents;
    }

    public String getDistrict()
    {
        return this.district;
    }

    public Child[] getChildren() {
        return children;
    }

    public Address getAddress() {
        return address;
    }

    public boolean isRegistered() {
        return isRegistered;
    }

    public String getId()
    {
        return this.id;
    }

    public String getLastName()
    {
        return this.lastName;
    }
}

