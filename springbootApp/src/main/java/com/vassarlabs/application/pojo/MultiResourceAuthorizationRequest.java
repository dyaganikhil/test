package com.vassarlabs.application.pojo;

//import lombok.Data;
import java.util.List;
//@Data
public class MultiResourceAuthorizationRequest {
    private String userId;
    private String applicationId;
    List<ResourceAuthorizationRequest> multiResourceAuthRequest;

    public void setUserId(String userId){
        this.userId = userId;
    }
    public void setApplicationId(String applicationId){
        this.applicationId = applicationId;
    }
    public void setMultiResourceAuthRequest(List<ResourceAuthorizationRequest> multiResourceAuthRequest){
        this.multiResourceAuthRequest = multiResourceAuthRequest;
    }
}
