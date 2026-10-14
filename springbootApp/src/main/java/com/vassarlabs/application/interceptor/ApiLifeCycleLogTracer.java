package com.vassarlabs.application.interceptor;

import com.vassarlabs.common.logging.api.MdcContextManager;
import com.vassarlabs.common.utils.StringUtils;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class ApiLifeCycleLogTracer extends OncePerRequestFilter {

    public static final String HEADER_NAME = "trace-id";

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain)
            throws ServletException, IOException {

        String cid = extractCorrelationId(request);
        boolean bound = MdcContextManager.tryBindManual(cid);

        //expose trace id to the client
        response.setHeader(HEADER_NAME, cid);
        try {
            filterChain.doFilter(request, response);
        } finally {
            if (bound) {
                MdcContextManager.clearIfOwner(
                        MdcContextManager.OWNER_MANUAL);
            }
        }
    }

    private String extractCorrelationId(HttpServletRequest request) {

        String traceID = request.getHeader(HEADER_NAME);

        if (StringUtils.isNullOrEmpty(traceID)) {
            traceID =  UUID.randomUUID().toString();
        }

        return traceID;
    }
}