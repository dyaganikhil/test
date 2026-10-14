package com.vassarlabs.application;

import com.vassarlabs.common.init.err.AppInitializationException;
import com.vassarlabs.common.init.service.RESTInitServiceImpl;
import com.vassarlabs.common.init.service.api.IApplicationInitService;
import com.vassarlabs.config.spring.AppContext;
import com.vassarlabs.config.spring.PlatformApplicationProperties;
import com.vassarlabs.kflink.pipeline.CPUProcessingPipeline;
import com.vassarlabs.location.service.impl.LocationHierarchyServiceImpl;

import javax.management.RuntimeErrorException;

public class CPUProcessingJobs {

    public static void main(String[] args) throws Throwable {
        try {
            IApplicationInitService applicationInitService = null;
            applicationInitService = AppContext.getApplicationContext().getBean(RESTInitServiceImpl.class);
            applicationInitService.initialize();
            System.out.println("ApplicationRun Started!!");

            //uncomment this for running REST APIs through spring boot app
            //SpringApplication.run(ApplicationRun.class, args);

            /*
             * uncomment this for running flink pipeline.
             * As part of creating Sink objects this will initialize quartz scheduler
             * AVLFlinkSink.initApplication -> StormInitServiceImpl.initialize -> QuartzSchedulerInitializer.initialize
             */
            CPUProcessingPipeline createPipeline = AppContext.getApplicationContext().getBean(CPUProcessingPipeline.class);
            createPipeline.buildAndSubmitPipelines();

            //CreateTopology createTopology = AppContext.getApplicationContext().getBean(CreateTopology.class);
            //createTopology.buildAndSubmitTopologies();

            test();

        } catch (AppInitializationException e) {
            throw new RuntimeErrorException(new Error(e),
                    "Error initializing application");
        }
    }

    private static void test() {
        try {
            PlatformApplicationProperties platformProps = AppContext.getApplicationContext().getBean(PlatformApplicationProperties.class);

            LocationHierarchyServiceImpl locService = AppContext.getApplicationContext().getBean(LocationHierarchyServiceImpl.class);
            System.out.println("XXXXXXXXXX: " + locService.getLocationTypeIDNameMap());
            System.out.println("XXXXXXXXXX: " + locService.getLocNameForLocUUID("9553a7af-56f8-495f-ba0c-c2d2324e6759",
                    platformProps.getProject()));

            //Map<String, List<String>> parentChildMap =
            //locService.getAllLocForParentChildTypes(LocationTypeConstants.STATE_TEXT, LocationTypeConstants.RAINFALL_TEXT);

            //List<String> stationUUIDs = parentChildMap.get(ParentEntityConstants.KW_TELANGANA);
            //System.out.println(stationUUIDs.size());


            //WORKING
            //APAWSScraper apAwsScraper = AppContext.getApplicationContext().getBean(APAWSScraper.class);
            //apAwsScraper.executeParser();

            //APAWSEODScraper apAwsEodScraper = AppContext.getApplicationContext().getBean(APAWSEODScraper.class);
            //apAwsEodScraper.executeParser();

            //TelanganaAWSEODScraper tsAwsEodScraper = AppContext.getApplicationContext().getBean(TelanganaAWSEODScraper.class);
            //tsAwsEodScraper.executeParser();

            //TelanganaAWSScraper tsAwsScraper = AppContext.getApplicationContext().getBean(TelanganaAWSScraper.class);
            //tsAwsScraper.executeParser();

            //OdishaAWSScraper odAwsScraper = AppContext.getApplicationContext().getBean(OdishaAWSScraper.class);
            //odAwsScraper.executeParser();

            //IMDGridFileParser imdGridScraper = AppContext.getApplicationContext().getBean(IMDGridFileParser.class);
            //imdGridScraper.executeParser();

            //IMDAWSScraperParser imdAwsScraper = AppContext.getApplicationContext().getBean(IMDAWSScraperParser.class);
            //imdAwsScraper.executeParser();

            //IMDARGScraperParser imdArgScraper = AppContext.getApplicationContext().getBean(IMDARGScraperParser.class);
            //imdArgScraper.executeParser();


            //NOT WORKING - NO LOGS
            //ChhatisgarhAWSScraper chgAwsScraper = AppContext.getApplicationContext().getBean(ChhatisgarhAWSScraper.class);
            //chgAwsScraper.executeParser();

            //NOT WORKING - NO LOGS in klsw deployment also
            //KarnatakaRainfallScraper ktkAwsScraper = AppContext.getApplicationContext().getBean(KarnatakaRainfallScraper.class);
            //ktkAwsScraper.executeParser();

            //NOT WORKING - ERROR in klsw deployment also
            //MaharashtraAWSScraper mhAwsScraper = AppContext.getApplicationContext().getBean(MaharashtraAWSScraper.class);
            //mhAwsScraper.executeParser();

            //NOT WORKING - NO LOGS in klsw deployment also
            //BhimaBasinRainfallScrapper bhimaBasinScraper = AppContext.getApplicationContext().getBean(BhimaBasinRainfallScrapper.class);
            //bhimaBasinScraper.executeParser();


             /*IKafkaEventNotifierServiceHandler kafkEventNotifierServiceHandler = AppContext.getApplicationContext().getBean(IKafkaEventNotifierServiceHandler.class);
			kafkEventNotifierServiceHandler.initializeKafkaConsumer(5);
			KafkaEventController kafkaEvent = new KafkaEventController();
			kafkaEvent.setSubject(kafkEventNotifierServiceHandler);
			kafkaEvent.setWebSocketEventNotifier(ctx.getBean(WebSocketEventNotifier.class));*/
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
