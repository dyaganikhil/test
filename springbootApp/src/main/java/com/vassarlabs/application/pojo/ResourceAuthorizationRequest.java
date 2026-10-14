package com.vassarlabs.application.pojo;
//import lombok.Data;

import jakarta.validation.metadata.Scope;
import java.util.ArrayList;
import java.util.List;

//@Data
public class ResourceAuthorizationRequest {
    private String resourceName;
    private List<String> actions;

    private List<Scope> scope;
    public void setResourceName(String resourceName){
        this.resourceName = resourceName;
    }

    public void setActions(List<String> actions){
        this.actions = actions;
    }

    public void setScope(ArrayList scopeList) { this.scope = scopeList; }
}
