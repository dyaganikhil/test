package com.vassarlabs.application;

import com.vassarlabs.application.config.BusinessComponentScan;
import com.vassarlabs.application.config.PlatformComponentScan;
import com.vassarlabs.common.init.err.AppInitializationException;
import com.vassarlabs.common.init.service.RESTInitServiceImpl;
import com.vassarlabs.common.init.service.api.IApplicationInitService;
import com.vassarlabs.config.spring.AppContext;
import com.vassarlabs.config.spring.PlatformApplicationProperties;
import com.vassarlabs.location.service.api.ILocationHierarchyService;
import com.vassarlabs.location.service.impl.LocationHierarchyServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.management.RuntimeErrorException;
import java.util.Arrays;


//@SpringBootApplication(scanBasePackages = {"com.vassarlabs.iwm.controllers.*","com.vassarlabs.iwm.timerange.service.controllers","com.vassarlabs.taskmngr.controller",
//        "com.vassarlabs.iwm.boundarydetection.controllers","com.vassarlabs.application.interceptor","com.vassarlabs.application.service","com.vassarlabs.application.exceptions",
//        "com.vassarlabs.pictorialanalysis.controllers","com.vassarlabs.cropanalysis.controllers","com.vassarlabs.iwm.sdp.controllers","com.vassarlabs.mea.controllers",
//        "com.vassarlabs.iwm.boundarydetection.config","com.vassarlabs.iwm.weather.service.controllers","com.vassarlabs.soilhealthcard.controllers","com.vassarlabs.iwm.rainfall.controllers",
//        "com.vassarlabs.iwm.temperature.controllers","com.vassarlabs.iwm.peststress.controllers","com.vassarlabs.humidity.controllers","com.vassarlabs.wbis.controller",
//        "com.vassarlabs.modelinventory.controller","com.vassarlabs.sdp.controller","com.vassarlabs.croplibrary.*","com.vassarlabs.pestlibrary.*","com.vassarlabs.icm.cswb.controllers",
//        "com.vassarlabs.iwm.wind.controllers","com.vassarlabs.iwm.soilmoisture.controllers","com.vassarlabs.iwm.cass.et.controllers","com.vassarlabs.iwm.vicmodel.controllers","com.vassarlabs.iwm.cropwaterdemand.controllers",
//        "com.vassarlabs.icm.waterstress.controllers","com.vassarlabs.icm.masterdata.farm.controllers","com.vassarlabs.icm.production.controllers","com.vassarlabs.icm.masterdata.farmer.controllers",
//        "com.vassarlabs.iwm.common.report.service.controllers","com.vassarlabs.notification.controllers","com.vassarlabs.elibrary.service.controllers","com.vassarlabs.icm.acz.controllers",
//        "com.vassarlabs.farmmechanization.controllers","com.vassarlabs.surveymgmt.controllers","com.vassarlabs.schemes.controller","com.vassarlabs.farmanalytics.controllers","com.vassarlabs.elasticsearch.*",
//        "com.vassarlabs.common.minio.service.controllers","com.vassarlabs.common.bhashiniintegration.controller"})

@SpringBootApplication
@Import({PlatformComponentScan.class, BusinessComponentScan.class})
//@EnableSwagger2
public class ApplicationRun {

    @Value(value = "${cors.allowed.origins}")
    String allowedOrigins;

    @Value("${application.prod.activate}")
    boolean isProdActive;

//    private static ApplicationContext ctx;
//    private static final Logger logger = LoggerFactory.getLogger("default");

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder.build();
    }

    public static void main(String[] args) {
        try {
            Long start = System.currentTimeMillis();
            System.out.println("Start Time :: " + start);


//            IApplicationInitService applicationInitService;
//            applicationInitService = AppContext.getApplicationContext().getBean(RESTInitServiceImpl.class);
//            applicationInitService.initialize();


            SpringApplication.run(ApplicationRun.class, args);
            Long end = System.currentTimeMillis();
            System.out.println("time taken :: " + (end -start));

//            test();
            System.out.println("Max Heap: " + Runtime.getRuntime().maxMemory() / (1024*1024) + " MB");

        } catch (Exception e) {
            throw new RuntimeErrorException(new Error(e),
                    "Error initializing application");
        }
    }

    private static void test() {
        try {
            PlatformApplicationProperties platformProps = AppContext.getApplicationContext().getBean(PlatformApplicationProperties.class);
            System.out.println(platformProps.getProject());
            System.out.println(platformProps.getCassDataStoreList());

            ILocationHierarchyService locService = AppContext.getApplicationContext().getBean(LocationHierarchyServiceImpl.class);
            System.out.println("XXXXXXXXXX: "+locService.getLocationTypeIDNameMap());
            System.out.println("XXXXXXXXXX: "+locService.getLocNameForLocUUID("9553a7af-56f8-495f-ba0c-c2d2324e6759", 
                platformProps.getProject()));
            System.out.println("Platform App Properties root entity uuid :: "+platformProps.getRootEntityUUID());


            

        }
        catch (Throwable t) {
            t.printStackTrace();
        }

    }

//    @Bean
//    public Docket productApi() {
//        return new Docket(DocumentationType.SWAGGER_2).select()
//                .apis(RequestHandlerSelectors.any())
//                .build();
//    }

    @Bean
    WebMvcConfigurer corsConfigurer() {

        String [] origins = Arrays.stream(allowedOrigins.split(",")).map(String::trim).toArray(String[]::new);
        System.out.println("allowed origins ::: " + Arrays.toString(origins));
        System.out.println("isProdActive :: " + isProdActive);
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/**")
                        .allowedMethods("GET", "POST", "PUT", "OPTIONS", "DELETE")
                        .allowedOrigins(origins)
                        .allowedHeaders("*").allowCredentials(false);
            }
        };
    }

}
