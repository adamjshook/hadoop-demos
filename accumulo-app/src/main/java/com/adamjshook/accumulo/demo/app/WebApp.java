package com.adamjshook.accumulo.demo.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.embedded.EmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

@ComponentScan
@EnableAutoConfiguration
public class WebApp
{
    private static Logger LOG = LoggerFactory.getLogger(WebApp.class);

    public static void main(String[] args)
    {
        SpringApplication.run(WebApp.class, args);
    }

    @Bean
    protected ServletContextListener listener()
    {
        return new ServletContextListener()
        {
            @Override
            public void contextInitialized(ServletContextEvent sce)
            {
                LOG.info("ServletContext initialized");
            }

            @Override
            public void contextDestroyed(ServletContextEvent sce)
            {
                LOG.info("ServletContext destroyed");
            }
        };
    }

    @Bean
    public EmbeddedServletContainerFactory servletContainer()
    {
        // return new TomcatEmbeddedServletContainerFactory(port);
        return new TomcatEmbeddedServletContainerFactory(8888);
    }

}
