package com.streamlite.broker.auth;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final ClientAuthInterceptor interceptor;

    public WebConfig(
            @Autowired
            ClientAuthInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(interceptor)
                .excludePathPatterns("/health")
                .excludePathPatterns("/public/**")
                .addPathPatterns("/user/*/test");
    }

}
