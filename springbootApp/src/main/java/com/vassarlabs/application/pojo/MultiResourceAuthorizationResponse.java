package com.vassarlabs.application.pojo;
//import lombok.Data;

import java.util.List;

//@Data
public class MultiResourceAuthorizationResponse {
    private String userId;
    private String applicationId;
    private List<ResourceAuthorizationResponse> resourceAccessInfo;

    public List<ResourceAuthorizationResponse> getResourceAccessInfo(){
        return resourceAccessInfo;
    }

    public void setApplicationId(String applicationId){
        this.applicationId = applicationId;
    }
    public void setUserId(String userId){
        this.userId = userId;
    }
}
