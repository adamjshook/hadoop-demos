package com.adamjshook.accumulo.demo.app;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.View;
import org.springframework.web.servlet.ViewResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.view.ContentNegotiatingViewResolver;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import java.util.Arrays;

@Component
public class WebConfig
        extends WebMvcConfigurerAdapter
{
    @Bean
    public ViewResolver viewResolver()
    {
        ContentNegotiatingViewResolver contentNegotiatingViewResolver =
                new ContentNegotiatingViewResolver();
        View jacksonView = new MappingJackson2JsonView();
        contentNegotiatingViewResolver.setDefaultViews(Arrays.asList(jacksonView));
        return contentNegotiatingViewResolver;
    }
}
