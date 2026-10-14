package com.vassarlabs.application.exceptions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.config.impl.APIResponse;
import com.vassarlabs.config.service.APIResponseService;
import com.vassarlabs.config.spring.AppContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PSQLException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.client.RestClientException;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.SQLException;

/**
 * This is the global exception handler for the entire application.
 * Both language defined and custom exceptions are handled here.
 */
@ControllerAdvice
@RestControllerAdvice
public class GlobalExceptionHandlerAdviceController {


    com.vassarlabs.config.service.APIResponseService APIResponseService;
    private APIResponseService getAPIResponseService() {
        if (APIResponseService == null) {
            APIResponseService = AppContext.getApplicationContext().getBean(APIResponseService.class);
        }
        return APIResponseService;
    }
    public GlobalExceptionHandlerAdviceController() {
        LOGGER.info("GlobalExceptionHandlerAdviceController initialized");
    }
    private static final Logger LOGGER = LogManager.getLogger(GlobalExceptionHandlerAdviceController.class);
    public static final String INTERNAL_SERVER_ERROR_MSG = "Something went wrong. Please try again later.";
    //Handler for Specific Runtime Exceptions
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler({ NullPointerException.class, IOException.class, SQLException.class, DSPException.class})
    public APIResponse handleSpecificExceptions (HttpServletRequest request, Exception ex) {
        LOGGER.debug("Inside handler of specific runtime exceptions");
        LOGGER.error("Request endpoint " + request.getRequestURL() + " Exception " + ex.getMessage());

        ex.printStackTrace();

        //Log the class name and method name where exception occurred
        this.logExceptionLocation(ex);

        //Build the api response
        return getAPIResponseService().buildAPIResponse(HttpStatus.INTERNAL_SERVER_ERROR, INTERNAL_SERVER_ERROR_MSG);
    }

    @ExceptionHandler({ PSQLException.class, Throwable.class})
    public APIResponse handleInterceptorExceptionsWithoutStatus (HttpServletRequest request, Exception ex) {
        LOGGER.debug("Inside handler of specific runtime exceptions");
        LOGGER.error("Request endpoint " + request.getRequestURL() + " Exception " + ex.getMessage());

        ex.printStackTrace();

        //Log the class name and method name where exception occurred
        this.logExceptionLocation(ex);

        //Build the api response
        return getAPIResponseService().buildAPIResponse(HttpStatus.INTERNAL_SERVER_ERROR, INTERNAL_SERVER_ERROR_MSG);
    }


    //Handler for Other Runtime Exceptions
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler({
            DataIntegrityViolationException.class
            , DataRetrievalFailureException.class
    })
    public APIResponse handleDataIntegrityViolationExceptions (HttpServletRequest request, Exception ex) {
        LOGGER.debug("Inside handler of Data integrity violation exceptions");
        LOGGER.error("Request endpoint " + request.getRequestURL() + " Exception " + ex.getMessage());
        ex.printStackTrace();

        //Log the class name and method name where exception occurred
        this.logExceptionLocation(ex);

        //Build the api response
        return getAPIResponseService().buildAPIResponse(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
    }


    //Handler for Other Runtime Exceptions
    @ExceptionHandler({ Exception.class})
    public APIResponse handleOtherExceptions (HttpServletRequest request, Exception ex) {
        LOGGER.debug("Inside handler of other generic runtime exceptions");
        LOGGER.error("Request endpoint " + request.getRequestURL() + ex.getClass().getName() + " Exception " + ex.getMessage());
        ex.printStackTrace();

        //Log the class name and method name where exception occurred
        this.logExceptionLocation(ex);

        //Build the api response
        return getAPIResponseService().buildAPIResponse(HttpStatus.INTERNAL_SERVER_ERROR, INTERNAL_SERVER_ERROR_MSG);
    }

    //Method which logs the class and method name where the exception occurred
    //N.B : Only the top level exception is considered
    private void logExceptionLocation(Exception ex) {
        //Check if stack trace exists for the method before logging it
        if (ex.getStackTrace().length > 0) {
            StackTraceElement stackTraceElement = ex.getStackTrace()[0];

            LOGGER.debug("Class Name " + stackTraceElement.getClassName());
            LOGGER.debug("Method Name " + stackTraceElement.getMethodName());
        }
        else {
            LOGGER.debug("No stack trace information found ");
        }
    }

    //method Invalid Rest client execption
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler({
            RestClientException.class,
            JsonProcessingException.class,
    })
    public APIResponse handleRestClientException (HttpServletRequest request, Exception ex) {
        LOGGER.debug("Inside handler of Rest Client Exception exceptions");
        LOGGER.error("Request endpoint " + request.getRequestURL() + " Exception " + ex.getMessage());
        ex.printStackTrace();

        //Log the class name and method name where exception occurred
        this.logExceptionLocation(ex);

        //Build the api response
        return getAPIResponseService().buildAPIResponse(HttpStatus.INTERNAL_SERVER_ERROR, INTERNAL_SERVER_ERROR_MSG);
    }

}
