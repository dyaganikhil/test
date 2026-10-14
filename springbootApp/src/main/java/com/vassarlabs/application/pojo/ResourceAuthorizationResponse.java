package com.vassarlabs.application.pojo;
//import lombok.Data;

import jakarta.validation.metadata.Scope;
import java.util.List;


//@Data
public class ResourceAuthorizationResponse {
    private String resourceName;
    private Boolean hasValidAccess;
    private List<String> actions;
    private List<Scope> scope;
    public Boolean getHasValidAccess(){
        return hasValidAccess;
    }

    public void setResourceName(String resourceName){
        this.resourceName = resourceName;
    }
    public void setActions(List<String> actions){
        this.actions = actions;
    }
    public void setScope(List<Scope> scope){
        this.scope = scope;
    }
    public void setHasValidAccess(Boolean hasValidAccess){
        this.hasValidAccess = hasValidAccess;
    }
}
