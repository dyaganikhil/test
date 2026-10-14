package com.vassarlabs.application.pojo;

//import lombok.Data;
//@Data
public class MultiResourceAuthorizationResponseWrapper {

        String result;
        String statusCode;
        String statusCodeDescription;
        String message;
        MultiResourceAuthorizationResponse response;

        public MultiResourceAuthorizationResponse getResponse(){
                return response;
        }
        public void setResult(String result){
               this.result=result;
        }
        public void setStatusCode(String statusCode){
                this.statusCode=statusCode;
        }
        public void setStatusCodeDescription(String statusCodeDescription){
                this.statusCodeDescription=statusCodeDescription;
        }
        public void setMessage(String message){
                this.message=message;
        }
}
