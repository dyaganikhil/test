package com.vassarlabs.application;

import com.vassarlabs.common.init.err.AppInitializationException;
import com.vassarlabs.common.init.service.RESTInitServiceImpl;
import com.vassarlabs.common.init.service.api.IApplicationInitService;
import com.vassarlabs.config.BusinessComponentScan;
import com.vassarlabs.config.PlatformComponentScan;
import com.vassarlabs.config.spring.AppContext;
import com.vassarlabs.config.spring.PlatformApplicationProperties;
import com.vassarlabs.kflink.pipeline.TrainingPipeline;
import com.vassarlabs.location.service.impl.LocationHierarchyServiceImpl;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import javax.management.RuntimeErrorException;

public class TrainingJobs {

    public static void main(String[] args) {
        try {

            AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
                    PlatformComponentScan.class,
                    BusinessComponentScan.class
            );

            // AppContext.setApplicationContext(context);

            // IApplicationInitService applicationInitService = context.getBean(RESTInitServiceImpl.class);
            // applicationInitService.initialize();

            System.out.println("ApplicationRun Started!!");

            TrainingPipeline createPipeline = context.getBean(TrainingPipeline.class);
            createPipeline.buildAndSubmitPipelines();

            // test();

        } catch (AppInitializationException e) {
            e.printStackTrace();
            throw new RuntimeErrorException(new Error(e), "Error initializing application");
        }
    }

    private static void test() {
        try {
            PlatformApplicationProperties platformProps = AppContext.getApplicationContext().getBean(PlatformApplicationProperties.class);
            System.out.println("project UUID :: " + platformProps.getRootEntityUUID());
            LocationHierarchyServiceImpl locService = AppContext.getApplicationContext().getBean(LocationHierarchyServiceImpl.class);
            System.out.println("locTypeIDNameMap :: " + locService.getLocationTypeIDNameMap());
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
