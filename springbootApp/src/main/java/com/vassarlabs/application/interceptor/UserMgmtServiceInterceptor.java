package com.vassarlabs.application.interceptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.vassarlabs.application.pojo.MultiResourceAuthorizationRequest;
import com.vassarlabs.application.pojo.MultiResourceAuthorizationResponse;
import com.vassarlabs.application.pojo.MultiResourceAuthorizationResponseWrapper;
import com.vassarlabs.application.pojo.ResourceAuthorizationRequest;
import com.vassarlabs.application.service.UserMgmtRestTemplateService;
import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.utils.CollectionUtils;
import com.vassarlabs.common.utils.StringUtils;
import com.vassarlabs.config.spring.AppContext;
import com.vassarlabs.usermgmt.pojo.api.IResource;
import com.vassarlabs.usermgmt.service.api.IUserMgmtDataService;
import com.vassarlabs.usermgmt.utils.UserManagementConstants;
import com.vassarlabs.usermgmt.utils.UserManagementUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class UserMgmtServiceInterceptor implements HandlerInterceptor {

    private static final long CACHE_TTL_MILLIS = TimeUnit.MINUTES.toMillis(5);

    private final Set<String> cachedResourceNames = new HashSet<>();
    private volatile long lastCacheGenTs = 0L;

    @Autowired
    private UserMgmtRestTemplateService userMgmtRestTemplateService;

    @Autowired
    private Gson gson;

    private IUserMgmtDataService userMgmtDataService;
    private IUserMgmtDataService getUserMgmtDataService() {
        if (userMgmtDataService == null) {
            userMgmtDataService = AppContext.getApplicationContext().getBean(IUserMgmtDataService.class);
        }
        return userMgmtDataService;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(UserMgmtServiceInterceptor.class);

    @Value("${application.prod.activate}")
    private boolean isProdActive;


    @Override
    public boolean preHandle
            (HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        try{
            long startTime = System.currentTimeMillis();
            boolean isExtAuthRequired = Boolean.parseBoolean(request.getHeader("extAuthorization"));
            System.out.println("VAXX :: Inside interceptor of play............................................................");
            Runtime rt = Runtime.getRuntime();
            long used = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);

//            LOGGER.info("[STARTUP] TaskManager: " + " | Heap used: " + used + "MB");

            for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
//                LOGGER.info("[STARTUP] Pool: " + pool.getName() + " | Used: " + (pool.getUsage().getUsed() / (1024 * 1024)) + "MB | Committed: " + (pool.getUsage().getCommitted() / (1024 * 1024)) +" MB | Max: " + (pool.getUsage().getMax() / (1024 * 1024)) + "MB");
            }

            if (!isProdActive && !isExtAuthRequired) {
                return true;
            }

            // If a valid API key is provided, resource authorization will be bypassed
            String apiKey = request.getHeader("X-API-Key");
            UserManagementConstants umConstants = AppContext.getApplicationContext().getBean(UserManagementConstants.class);
            if (!StringUtils.isNullOrEmpty(apiKey) && apiKey.equals(umConstants.getAPI_KEY())) {
                return true;
            }

//            System.out.println("request params :: " + request);
            System.out.println("Request Method: " + request.getMethod());
            System.out.println("Request URI: " + request.getRequestURI());
//            System.out.println("Request URL: " + request.getRequestURL());
            Boolean isSaaS = request.getHeader("saas") != null && !request.getHeader("saas").isEmpty();

//            System.out.println("Headers:");
//            request.getHeaderNames().asIterator()
//                    .forEachRemaining(headerName -> System.out.println(headerName + ": " + request.getHeader(headerName)));

            System.out.println("Parameters:");
            request.getParameterMap().forEach((param, values) ->
                    System.out.println(param + ": " + String.join(", ", values)));

            String authTokenFromHeader = request.getHeader("Authorization");
            String csrfToken = request.getHeader("Csrf-Token");
            String userId = request.getHeader("userId");
            String locationAuthorizationData = request.getHeader("X-Loc-Auth-Data");
//            String  resourceName= request.getHeader("endpoints");
//            if(request.getMethod().equals("GET") || request.getMethod().equals("DELETE") || request.getMethod().equals("PUT") || request.getMethod().equals("POST")){
//                if(isSaaS){
//                    resourceName = "notYetConfigured";
//                }else {
//                    for(String apiName : UserMgmtConstants.apiNamesToEndPointNamesMap.keySet()){
//                        if(request.getRequestURI().contains(apiName)){
//                            resourceName = UserMgmtConstants.apiNamesToEndPointNamesMap.get(apiName);
//                            break;
//                        }
//                    }
//                }
//            }

            // Extracting resourceName from endpoint
            String path = "";
            try {
                HandlerMethod method = (HandlerMethod) handler;
                RequestMapping mapping = method.getMethodAnnotation(RequestMapping.class);
                if (mapping == null) return false;
                LOGGER.debug("Method Mapping value :: {}", Arrays.toString(mapping.value())); // [/change-password]
                String classContextPath = "";
                RequestMapping classMapping = method.getBeanType().getAnnotation(RequestMapping.class);
                if (classMapping != null) {
                    LOGGER.debug("Class Mapping value :: {}", Arrays.toString(classMapping.value()));
                    classContextPath = Arrays.toString(classMapping.value()).replace("[", "").replace("]", "");
                    if (classContextPath.endsWith("/")) classContextPath = classContextPath.substring(0, classContextPath.length() - 1);
                }
                path = Arrays.toString(mapping.value()).replace("[", "").replace("]", "");
                if (!StringUtils.isNullOrEmpty(classContextPath)) {
                    if (!path.startsWith("/")) path = "/" + path;
                    path = classContextPath + path;
                }
            } catch (Exception e) {
                LOGGER.debug("Exception :: {}", e.getMessage());
            }

            // Regex pattern to match path variables like /{variable} and replace with empty
            String regex = "/\\{[^}]+}";
            String resoureName = path.replaceAll(regex, "");

            // Remove leading slash if present
            if (resoureName.startsWith("/")) {
                resoureName = resoureName.substring(1);
            }
            System.out.println("resourceName :: " + resoureName);

            // Handle resource authorization if no access is received
            if (StringUtils.isNullOrEmpty(authTokenFromHeader)) {
                boolean isOpen = isOpenAPIResource(resoureName);
                if (!isOpen) {
                    response.getWriter().write("permission denied");
                }
                return isOpen;
            }

            String applicationId = request.getHeader("applicationId");

            String userMgmtUrl = umConstants.getUSERMGMT_URL();
            Map<String, String> requestOptions = userMgmtRestTemplateService.buildRequestOptions(authTokenFromHeader, csrfToken, userId, locationAuthorizationData);

            MultiResourceAuthorizationRequest requestBody = new MultiResourceAuthorizationRequest();
            List<ResourceAuthorizationRequest> resourceAuthorizationRequestList = new ArrayList<>();
            ResourceAuthorizationRequest resourceAuthorizationRequest = new ResourceAuthorizationRequest();
            resourceAuthorizationRequest.setResourceName(resoureName);
            resourceAuthorizationRequest.setActions(new ArrayList<>(Arrays.asList("READ","EDIT")));
            resourceAuthorizationRequest.setScope(new ArrayList<>(Arrays.asList()));
            resourceAuthorizationRequestList.add(resourceAuthorizationRequest);

            requestBody.setUserId(userId);
            requestBody.setApplicationId(applicationId);
            requestBody.setMultiResourceAuthRequest(resourceAuthorizationRequestList);
            String requestBodyStr = gson.toJson(requestBody);
//            System.out.println("VAXX :: Making call to user mgmt................................. requestBodyStr :: " + requestBodyStr);

            Long UMStartTime = System.currentTimeMillis();
            ResponseEntity<String> responseFromUserMgmt = null;
            try {
                responseFromUserMgmt = userMgmtRestTemplateService.makePostRequest(userMgmtUrl, requestOptions, requestBodyStr);
            } catch (RestClientResponseException e){
                e.printStackTrace();
                response.setStatus(e.getRawStatusCode());
                response.setHeader("csrfToken", csrfToken);
                response.getWriter().write(e.getResponseBodyAsString());
                return false;
            }

            Long UMEndTime = System.currentTimeMillis();
//            System.out.println("###########responseFromUserMgmt:::::: " + responseFromUserMgmt);
            System.out.println("***********************Time taken in user management :::::::: " + (UMEndTime-UMStartTime));
            ObjectMapper mapper = new ObjectMapper();

            MultiResourceAuthorizationResponseWrapper multiResourceAuthorizationResponseWrapper = mapper.readValue(responseFromUserMgmt.getBody(), MultiResourceAuthorizationResponseWrapper.class);
            Boolean hasValidAccess = ((MultiResourceAuthorizationResponse) multiResourceAuthorizationResponseWrapper
                    .getResponse()).getResourceAccessInfo().get(0).getHasValidAccess();
            System.out.println("hadValidAccess :: " + hasValidAccess);

//            String csrfTokenInResponse = String.valueOf(responseFromUserMgmt.getHeaders().get("csrfToken").get(0));
            response.setHeader("csrfToken", csrfToken);
            if (hasValidAccess) {
//                System.out.println("VAXX :: Allowing the api to proceed........................................................");
                System.out.println("*********************Time taken in WRIMS :::::::: " + (System.currentTimeMillis()-startTime));

//                System.out.println("VAXX :: Setting roleIds to request attribute");
                addResponseHeaderToRequestAttributes(responseFromUserMgmt, request);
                return true;
            }
            response.getWriter().write("permission denied");
            System.out.println("**************************Time taken in WRIMS :::::::: " + (System.currentTimeMillis()-startTime));
        }catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response,
                           Object handler, ModelAndView modelAndView) throws Exception {

        System.out.println("Post Handle method is Called..");
    }

    @Override
    public void afterCompletion
            (HttpServletRequest request, HttpServletResponse response, Object
                    handler, Exception exception) throws Exception {

        System.out.println("Request and Response is completed");
    }

    public boolean isOpenAPIResource(String resourceName) {
        LOGGER.info("isOpenAPIResource() :: resourceName :: {}", resourceName);
        try {
            refreshCacheIfRequired();
        } catch (DSPException e) {
            e.printStackTrace();
            return false;
        }

        return !StringUtils.isNullOrEmpty(resourceName) && cachedResourceNames.contains(resourceName);
    }

    private void refreshCacheIfRequired() throws DSPException {
        long refTs = System.currentTimeMillis();
        if (!CollectionUtils.isNullOrEmptySet(cachedResourceNames) && (refTs - lastCacheGenTs) < CACHE_TTL_MILLIS) {
            return;
        }

        synchronized (this) {
            // Double-check after acquiring lock
            if (refTs - lastCacheGenTs < CACHE_TTL_MILLIS) {
                return;
            }

            Map<String, IResource> resourceIdVsResource = getUserMgmtDataService().getResourceDetailsByResourceType(UserManagementUtils.COMMON_APIS_RESOURCE_TYPE);
            if (CollectionUtils.isNullOrEmptyMap(resourceIdVsResource)) {
                return;
            }

            Set<String> resourceNames = resourceIdVsResource.values().stream().map(IResource::getResourceName).collect(Collectors.toSet());
            cachedResourceNames.clear();
            cachedResourceNames.addAll(resourceNames);
            lastCacheGenTs = refTs;
        }
    }

    private void addResponseHeaderToRequestAttributes(ResponseEntity<String> response, HttpServletRequest request) {
        if (!response.getStatusCode().is2xxSuccessful()) {
            System.out.println("Error response returned...so returning");
            return;
        }
        HttpHeaders responseHeaders = response.getHeaders();
        if (responseHeaders.isEmpty()) {
            System.out.println("No response headers received..so returning");
            return;
        }

        // Extract roleIds and set it as an attribute
        String roleIdsStr = responseHeaders.getFirst(UserManagementUtils.ROLE_INFORMATION_ATTRIBUTE_NAME);
        if (StringUtils.isNullOrEmpty(roleIdsStr)) {
            System.out.println("Empty roleInfo present in response headers");
            return;
        }

        Map<String, String> roleInfo = gson.fromJson(roleIdsStr, new TypeToken<Map<String, String>>(){}.getType());
        if (CollectionUtils.isNullOrEmptyMap(roleInfo)) {
            System.out.println("Unable to get roleInfo from response headers");
            return;
        }
        request.setAttribute(UserManagementUtils.ROLE_INFORMATION_ATTRIBUTE_NAME, roleInfo);
    }
}
