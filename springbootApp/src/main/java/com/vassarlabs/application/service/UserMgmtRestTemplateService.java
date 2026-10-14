package com.vassarlabs.application.service;

import com.vassarlabs.application.constants.UserMgmtConstants;
import com.vassarlabs.common.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Component
public class UserMgmtRestTemplateService {

    @Autowired
    private RestTemplate restTemplate;

    public ResponseEntity<Object> makeGetRequest(String url, Map<String, String> options)
            throws RestClientException {
        HttpHeaders headerInfo = buildHttpHeaders(options);
        HttpEntity request = new HttpEntity(headerInfo);

        return restTemplate.exchange(url, HttpMethod.GET, request, Object.class);
    }

    public ResponseEntity<String> makePostRequest(String url, Map<String, String> options, String payload) throws RestClientException {

        HttpHeaders headerInfo = buildHttpHeaders(options);
        HttpEntity<String> request = new HttpEntity<String>(payload, headerInfo);

        return restTemplate.exchange(url, HttpMethod.POST, request, String.class);

    }
    public ResponseEntity<String> makePutRequest(
            String url,
            Map<String, String> options,
            String payload) throws RestClientException{
        HttpHeaders headerInfo = buildHttpHeaders(options);
        HttpEntity<String> request = new HttpEntity<String>(payload, headerInfo);

        return restTemplate.exchange(url, HttpMethod.PUT, request, String.class);
    }

    public ResponseEntity<Object> makeDeleteRequest(String url, Map<String, String> options)
            throws RestClientException{

        HttpHeaders headerInfo = buildHttpHeaders(options);
        HttpEntity request = new HttpEntity(headerInfo);
        return restTemplate.exchange(url, HttpMethod.DELETE, request, Object.class);

    }
    private static HttpHeaders buildHttpHeaders(Map<String, String> options) {
        HttpHeaders headers = new HttpHeaders();

        String isHeaderRequired = options.get(UserMgmtConstants.IS_HEADER_REQUIRED);

        //Set Content-Type Header if present
        if (options.containsKey(UserMgmtConstants.CONTENT_TYPE_LABEL)) {
            String contentType = options.get(UserMgmtConstants.CONTENT_TYPE_LABEL);
            headers.set(UserMgmtConstants.CONTENT_TYPE_LABEL, contentType);
        }

        //Set Authorization Header if required
        if (isHeaderRequired.equals(UserMgmtConstants.IS_TRUE)) {
            String accessToken = options.get(UserMgmtConstants.TOKEN_LABEL);
            String csrfToken = options.get(UserMgmtConstants.CSRF_TOKEN_LABEL);
            headers.set(UserMgmtConstants.AUTHORIZATION_HEADER, accessToken);
            headers.set(UserMgmtConstants.CSRF_TOKEN_LABEL, accessToken);
        }

        // Set userId header if present
        if (!StringUtils.isNullOrEmpty(options.get(UserMgmtConstants.USER_ID_LABEL))) {
            headers.set(UserMgmtConstants.USER_ID_LABEL, options.get(UserMgmtConstants.USER_ID_LABEL));
        }

        // Set location authorization data header if present
        if (!StringUtils.isNullOrEmpty(options.get(UserMgmtConstants.LOCATION_AUTH_DATA_LABEL))) {
            headers.set(UserMgmtConstants.LOCATION_AUTH_DATA_LABEL, options.get(UserMgmtConstants.LOCATION_AUTH_DATA_LABEL));
        }

//        System.out.println("headers :: " + headers.toString());

        return headers;
    }

    public Map<String, String> buildRequestOptions(
            String accessToken, String csrfToken, String userId, String locationAuthData) {
        Map<String, String> options = new HashMap<>();

        options.put(UserMgmtConstants.IS_HEADER_REQUIRED,UserMgmtConstants.IS_TRUE);
        options.put(UserMgmtConstants.CONTENT_TYPE_LABEL, MediaType.APPLICATION_JSON_VALUE);
        options.put(UserMgmtConstants.TOKEN_LABEL, accessToken);
        options.put(UserMgmtConstants.CSRF_TOKEN_LABEL, csrfToken);
        options.put(UserMgmtConstants.USER_ID_LABEL, userId);
        options.put(UserMgmtConstants.LOCATION_AUTH_DATA_LABEL, locationAuthData);

        return options;
    }

}
