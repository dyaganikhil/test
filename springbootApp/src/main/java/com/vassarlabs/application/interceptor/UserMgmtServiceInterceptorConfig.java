package com.vassarlabs.application.interceptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.*;

@Configuration
public class UserMgmtServiceInterceptorConfig implements WebMvcConfigurer {
    @Autowired
    UserMgmtServiceInterceptor userMgmtServiceInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(userMgmtServiceInterceptor);
    }
}
