package com.vassarlabs.kstorm.topology;

import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.config.service.api.IConfigPropertyRepoService;
import com.vassarlabs.iwm.aggr.bolts.CassAggrBolt;
import com.vassarlabs.scrapers.bolt.ScrapedDataEventBolt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/*import com.vassarlabs.icm.kstorm.bolt.ICMPropagationBolt;
import com.vassarlabs.iwm.actualWeather.service.kstorm.bolts.WeatherDataScrapingEventBolt;
import com.vassarlabs.iwm.canalmanagement.bolts.api.ICanalDataBolt;
import com.vassarlabs.iwm.canalmanagement.bolts.impl.CanalDataBolt;
import com.vassarlabs.iwm.common.report.service.kstorm.bolts.ReportGenerationEventBolt;
import com.vassarlabs.iwm.cyclone.service.kstorm.bolts.CycloneDownloadEventBolt;
import com.vassarlabs.iwm.humidity.kstorm.bolts.HumidityBolt;
import com.vassarlabs.iwm.humidiy.bolts.api.IHumidityBolt;
import com.vassarlabs.iwm.hydromodel.calibration.kstorm.bolts.CalibrationBolt;
import com.vassarlabs.iwm.hydromodel.floodforecast.kstorm.bolts.FloodForecastAggregationBolt;
import com.vassarlabs.iwm.hydromodel.floodforecast.kstorm.bolts.FloodForecastComputationBolt;
import com.vassarlabs.iwm.hydromodel.hecras.bolts.HecrasAggrBolt;
import com.vassarlabs.iwm.hydromodel.hecras.bolts.HecrasInputEventBolt;
import com.vassarlabs.iwm.hydromodel.hecras.bolts.HecrasOutputEventBolt;
import com.vassarlabs.iwm.hydromodel.hms.kstorm.bolts.HMSAggrBolt;
import com.vassarlabs.iwm.hydromodel.hms.kstorm.bolts.HMSInputBolt;
import com.vassarlabs.iwm.hydromodel.hms.kstorm.bolts.HMSOutputBolt;
import com.vassarlabs.iwm.hydromodel.vic.kstorm.bolts.CatchmentFlowAggrBolt;
import com.vassarlabs.iwm.hydromodel.vic.kstorm.bolts.VICDataEventBolt;
import com.vassarlabs.iwm.hydromodel.vic.kstorm.bolts.VICInputDataEventBolt;
import com.vassarlabs.iwm.hydromodel.vic.kstorm.bolts.VICRoutDataEventBolt;
import com.vassarlabs.iwm.kstorm.bolt.ModelDataEventBolt;
import com.vassarlabs.iwm.kstorm.bolt.TestDataEventBolt;
import com.vassarlabs.iwm.nowcast.service.kstorm.bolts.NowcastScrapingEventBolt;
import com.vassarlabs.iwm.pressure.bolts.api.IPressureBolt;
import com.vassarlabs.iwm.pressure.kstorm.bolts.PressureBolt;
import com.vassarlabs.iwm.pumphouse.bolts.PumpDataBolt;
import com.vassarlabs.iwm.pumphouse.bolts.api.IPumpDataBolt;
import com.vassarlabs.iwm.reservoir.bolts.ReservoirDataBolt;
import com.vassarlabs.iwm.reservoir.bolts.api.IReservoirDataBolt;
import com.vassarlabs.iwm.rivergauge.bolts.RiverGaugeDataBolt;
import com.vassarlabs.iwm.rivergauge.bolts.api.IRiverGuageDataBolt;
import com.vassarlabs.iwm.satellite.service.SatelliteDataDLEventBolt;
import com.vassarlabs.iwm.satellite.service.SatelliteDataProcessingBolt;
import com.vassarlabs.iwm.temperature.bolts.TemperatureBolt;
import com.vassarlabs.iwm.temperature.bolts.api.ITemperatureBolt;
import com.vassarlabs.iwm.weather.service.kstorm.bolts.WFAggregationEventBolt;
import com.vassarlabs.iwm.weather.service.kstorm.bolts.WFDownloadEventBolt;
import com.vassarlabs.iwm.wind.bolts.api.IWindBolt;
import com.vassarlabs.iwm.wind.kstorm.bolts.WindBolt;
*/

@Component
public class CreateTopology {

	@Autowired
	private IConfigPropertyRepoService configPropertyRepoService;

	protected void buildCassAggrEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm","CASS_AGGR_EVENT.NO_SPOUT_EXECUTORS", 4);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CASS_AGGR_EVENT.NO_BOLT_EXECUTORS", 8);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CASS_AGGR_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "CASS_AGGR_EVENT";
		String zkRoot = "/cass_aggr_event";
		String zkBoltId = "cass_aggr_event-bolt";
		String zkSpoutId = "cass_aggr_event-spout";

		CassAggrBolt cassAggrEventBolt = new CassAggrBolt();
		KafkaSpout<String, String> cassAggrEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, cassAggrEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, cassAggrEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}	


	protected void buildScraperEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "SCRAPER_EVENT.NO_SPOUT_EXECUTORS", 2);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "SCRAPER_EVENT.NO_BOLT_EXECUTORS", 4);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "SCRAPER.TASKS_MULTIPLIER", 1);

		String zkTopic = "SCRAPER_EVENT";
		String zkRoot = "/scraper_event";
		String zkSpoutId = "scraper_event-spout";
		String zkBoltId = "scraper_event-bolt";

		ScrapedDataEventBolt scrapedDataEventBolt = new ScrapedDataEventBolt();
		KafkaSpout<String, String> scraperEventKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, scraperEventKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, scrapedDataEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}


	protected void buildRainfallEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "RAINFALL_EVENT.NO_SPOUT_EXECUTORS", 2);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "RAINFALL_EVENT.NO_BOLT_EXECUTORS", 4);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "RAINFALL_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "RAINFALL_EVENT";
		String zkRoot = "/rainfall_event";
		String zkBoltId = "rainfall_event-bolt";
		String zkSpoutId = "rainfall_event-spout";
	
//		IRainfallBolt rfBolt = new RainfallBolt();
		KafkaSpout<String, String> rainfallKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, rainfallKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
//		builder.setBolt(zkBoltId, rfBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}

	/*
	protected void buildModelDataEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "MODEL_DATA_EVENT.NO_SPOUT_EXECUTORS", 2);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "MODEL_DATA_EVENT.NO_BOLT_EXECUTORS", 4);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "MODEL_DATA_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "MODEL_DATA_EVENT";
		String zkRoot = "/model_data_event";
		String zkBoltId = "model_data_event-bolt";
		String zkSpoutId = "model_data_event-spout";

		ModelDataEventBolt modelBolt = new ModelDataEventBolt();
		KafkaSpout<String, String> rainfallKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, rainfallKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, modelBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}
	

	protected void buildPumpHouseEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "PUMPHOUSE_EVENT.NO_SPOUT_EXECUTORS", 5);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "PUMPHOUSE_EVENT.NO_BOLT_EXECUTORS", 10);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "PUMPHOUSE_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "PUMPHOUSE_EVENT";
		String zkRoot = "/pumphouse_event";
		String zkBoltId = "pumphouse_event-bolt";
		String zkSpoutId = "pumphouse_event-spout";

		IPumpDataBolt phBolt = new PumpDataBolt();
		KafkaSpout<String, String> rainfallKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, rainfallKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, phBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}


	protected void buildReservoirEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "RESERVOIR_EVENT.NO_SPOUT_EXECUTORS", 5);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "RESERVOIR_EVENT.NO_BOLT_EXECUTORS", 15);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "RESERVOIR_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "RESERVOIR_EVENT";
		String zkRoot = "/reservoir_event";
		String zkBoltId = "reservoir_event-bolt";
		String zkSpoutId = "reservoir_event-spout";

		IReservoirDataBolt resBolt = new ReservoirDataBolt();
		KafkaSpout<String, String> reservoirKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, reservoirKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, resBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}
	

	protected void buildTemperatureEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "TEMPERATURE_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "TEMPERATURE_EVENT.NO_BOLT_EXECUTORS", 2);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "TEMPERATURE_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "TEMPERATURE_EVENT";
		String zkRoot = "/temperature_event";
		String zkSpoutId = "temperature_event-spout";
		String zkBoltId = "temperature_event-bolt";

		ITemperatureBolt tempBolt = new TemperatureBolt();
		KafkaSpout<String, String> tempKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, tempKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, tempBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}


	protected void buildHumidityEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HUMIDITY_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HUMIDITY_EVENT.NO_BOLT_EXECUTORS", 2);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HUMIDITY_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "HUMIDITY_EVENT";
		String zkRoot = "/humidity_event";
		String zkSpoutId = "humidity_event-spout";
		String zkBoltId = "humidity_event-bolt";

		IHumidityBolt humidityEventBolt = new HumidityBolt();
		KafkaSpout<String, String> humidityKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, humidityKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, humidityEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}


	protected void buildWindEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "WIND_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "WIND_EVENT.NO_BOLT_EXECUTORS", 2);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "WIND_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "WIND_EVENT";
		String zkRoot = "/wind_event";
		String zkSpoutId = "wind_event-spout";
		String zkBoltId = "wind_event-bolt";

		IWindBolt windBolt = new WindBolt();
		KafkaSpout<String, String> windKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, windKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, windBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}

	
	protected void buildPressureEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm","PRESSURE_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "PRESSURE_EVENT.NO_BOLT_EXECUTORS", 2);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "PRESSURE_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "PRESSURE_EVENT";
		String zkRoot = "/pressure_event";
		String zkSpoutId = "pressure_event-spout";
		String zkBoltId = "pressure_event-bolt";

		IPressureBolt pressureBolt = new PressureBolt();
		KafkaSpout<String, String> pressureKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, pressureKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, pressureBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}
	
	
	protected void buildRiverGaugeEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "RIVERGAUGE_EVENT.NO_SPOUT_EXECUTORS", 3);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "RIVERGAUGE_EVENT.NO_BOLT_EXECUTORS", 7);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "RIVERGAUGE_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "RIVERGAUGE_EVENT";
		String zkRoot = "/river_gauge_event";
		String zkBoltId = "river_gauge_event-bolt";
		String zkSpoutId = "river_gauge_event-spout";

		IRiverGuageDataBolt gaugeBolt = new RiverGaugeDataBolt();
		KafkaSpout<String, String> riverGaugeKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, riverGaugeKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, gaugeBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}
	

	protected void buildDownloadEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "SATELLITE_DOWNLOAD.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "SATELLITE_DOWNLOAD.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "SATELLITE_DOWNLOAD.TASKS_MULTIPLIER", 1);

		String zkTopic = "SATELLITE_DOWNLOAD";
		String zkRoot = "/satellite_download";
		String zkBoltId = "satellite_download-bolt";
		String zkSpoutId = "satellite_download-spout";

		SatelliteDataDLEventBolt satelliteDataDLEventBolt = new SatelliteDataDLEventBolt();
		KafkaSpout<String, String> satelliteDLEventKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, satelliteDLEventKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, satelliteDataDLEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}
	

	protected void buildProcessingEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "SATELLITE_PROCESSING.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "SATELLITE_PROCESSING.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "SATELLITE_PROCESSING.TASKS_MULTIPLIER", 1);

		String zkTopic = "SATELLITE_PROCESSING";
		String zkRoot = "/satellite_processing";
		String zkBoltId = "satellite_processing-bolt";
		String zkSpoutId = "satellite_processing-spout";

		SatelliteDataProcessingBolt satelliteProcessingEventBolt = new SatelliteDataProcessingBolt();
		KafkaSpout<String, String> satProcessingKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, satProcessingKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, satelliteProcessingEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}
	

	protected void buildForecastDownloadEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "FORECAST_DOWNLOAD.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "FORECAST_DOWNLOAD.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "FORECAST_DOWNLOAD.TASKS_MULTIPLIER", 1);

		String zkTopic = "FORECAST_DOWNLOAD";
		String zkRoot = "/forecast_download";
		String zkSpoutId = "forecast_download-spout";
		String zkBoltId = "forecast_download-bolt";

		WFDownloadEventBolt wfDownloadEventBolt = new WFDownloadEventBolt();
		KafkaSpout<String, String> wfDownloadEventKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, wfDownloadEventKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, wfDownloadEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}


	public void buildForecastAggregationEventTopologyPartial(TopologyBuilder builder, String topologyName){
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "FORECAST_AGGREGATION.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "FORECAST_AGGREGATION.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "FORECAST_AGGREGATION.TASKS_MULTIPLIER", 1);

		String zkTopic = "FORECAST_AGGREGATION";
		String zkRoot = "/forecast_aggregation";
		String zkSpoutId = "forecast_aggregation-spout";
		String zkBoltId = "forecast_aggregation-bolt";

		WFAggregationEventBolt wFAggregationEventBolt = new WFAggregationEventBolt();
		KafkaSpout<String, String> wfAggregationEventKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, wfAggregationEventKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors*taskMulttiplier);
		builder.setBolt(zkBoltId, wFAggregationEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors*taskMulttiplier).shuffleGrouping(zkSpoutId);
	}

	
	protected void buildCanalEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CANAL_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CANAL_EVENT.NO_BOLT_EXECUTORS", 2);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CANAL_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "CANAL_EVENT";
		String zkRoot = "/canal_event";
		String zkBoltId = "canal_event-bolt";
		String zkSpoutId = "canal_event-spout";

		ICanalDataBolt canalBolt = new CanalDataBolt();
		KafkaSpout<String, String> canalKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, canalKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, canalBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}

	
	protected void buildICMPropagationEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "ICM_PROPAGATION_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "ICM_PROPAGATION_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "ICM_PROPAGATION_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "ICM_PROPAGATION_EVENT";
		String zkRoot = "/icm_propagation_event";
		String zkSpoutId = "icm_propagation_event-spout";
		String zkBoltId = "icm_propagation_event-bolt";

		ICMPropagationBolt icmPropagationEventBolt = new ICMPropagationBolt();
		KafkaSpout<String, String> icmPropagationEventKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, icmPropagationEventKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, icmPropagationEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}
	
	
	public void buildCycloneDownloadEventTopologyPartial(TopologyBuilder builder, String topologyName){
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CYCLONE_DOWNLOAD.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CYCLONE_DOWNLOAD.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CYCLONE_DOWNLOAD.TASKS_MULTIPLIER", 1);

		String zkTopic = "CYCLONE_DOWNLOAD";
		String zkRoot = "/cyclone_download";
		String zkSpoutId = "cyclone_download-spout";
		String zkBoltId = "cyclone_download-bolt";

		CycloneDownloadEventBolt cycloneDownloadEventBolt = new CycloneDownloadEventBolt();
		KafkaSpout<String, String> cycloneDownloadEventKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);		
		builder.setSpout(zkSpoutId, cycloneDownloadEventKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors*taskMulttiplier);
		builder.setBolt(zkBoltId, cycloneDownloadEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors*taskMulttiplier).shuffleGrouping(zkSpoutId);
	}


	public void buildWeatherDataScrapingEventTopologyPartial(TopologyBuilder builder, String topologyName){
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "WEATHER_DATA_SCRAPING.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "WEATHER_DATA_SCRAPING.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "WEATHER_DATA_SCRAPING.TASKS_MULTIPLIER", 1);

		String zkTopic = "WEATHER_DATA_SCRAPING";
		String zkRoot = "/weather_data_scraping";
		String zkSpoutId = "weather_data_scraping-spout";
		String zkBoltId = "weather_data_scraping-bolt";

		WeatherDataScrapingEventBolt weatherDataScrapingEventBolt = new WeatherDataScrapingEventBolt();
		KafkaSpout<String, String> weatherDataScrapingEventKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);		
		builder.setSpout(zkSpoutId, weatherDataScrapingEventKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors*taskMulttiplier);
		builder.setBolt(zkBoltId, weatherDataScrapingEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors*taskMulttiplier).shuffleGrouping(zkSpoutId);
	}
	

	public void buildNowcastScrapingEventTopologyPartial(TopologyBuilder builder, String topologyName){
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "NOWCAST_SCRAPING.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "NOWCAST_SCRAPING.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "NOWCAST_SCRAPING.TASKS_MULTIPLIER", 1);

		String zkTopic = "NOWCAST_SCRAPING";
		String zkRoot = "/nowcast_scraping";
		String zkSpoutId = "nowcast_scraping-spout";
		String zkBoltId = "nowcast_scraping-bolt";

		NowcastScrapingEventBolt nowcastScrapingEventBolt = new NowcastScrapingEventBolt();
		KafkaSpout<String, String> nowcastScrapingEventKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);		
		builder.setSpout(zkSpoutId, nowcastScrapingEventKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors*taskMulttiplier);
		builder.setBolt(zkBoltId, nowcastScrapingEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors*taskMulttiplier).shuffleGrouping(zkSpoutId);
	}

	
	public void buildReportGenerationEventTopologyPartial(TopologyBuilder builder, String topologyName){
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "REPORT_GENERATION.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "REPORT_GENERATION.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "REPORT_GENERATION.TASKS_MULTIPLIER", 1);

		String zkTopic = "REPORT_GENERATION";
		String zkRoot = "/report_generation";
		String zkSpoutId = "report_generation-spout";
		String zkBoltId = "report_generation-bolt";

		ReportGenerationEventBolt reportGenerationEventBolt = new ReportGenerationEventBolt();
		KafkaSpout<String, String> reportGenerationEventKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, reportGenerationEventKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors*taskMulttiplier);
		builder.setBolt(zkBoltId, reportGenerationEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors*taskMulttiplier).shuffleGrouping(zkSpoutId);
	}


	protected void buildVICEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "VIC_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "VIC_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "VIC_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "VIC_EVENT";
		String zkRoot = "/vic_event";
		String zkSpoutId = "vic_event-spout";
		String zkBoltId = "vic_event-bolt";

		VICDataEventBolt vicDataEventBolt = new VICDataEventBolt();
		KafkaSpout<String, String> vicEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, vicEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, vicDataEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}
	

	protected void buildVICInputEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "VIC_INPUT_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "VIC_INPUT_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "VIC_INPUT_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "VIC_INPUT_EVENT";
		String zkRoot = "/vic_input_event";
		String zkSpoutId = "vic_input_event-spout";
		String zkBoltId = "vic_input_event-bolt";

		VICInputDataEventBolt vicDataEventBolt = new VICInputDataEventBolt();
		KafkaSpout<String, String> vicEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, vicEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, vicDataEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}

	
	protected void buildVICRoutEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "VIC_OUTPUT_ROUT_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "VIC_OUTPUT_ROUT_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "VIC_OUTPUT_ROUT_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "VIC_OUTPUT_ROUT_EVENT";
		String zkRoot = "/vic_output_rout_event";
		String zkSpoutId = "vic_output_rout_event-spout";
		String zkBoltId = "vic_output_rout_event-bolt";

		VICRoutDataEventBolt vicDataEventBolt = new VICRoutDataEventBolt();
		KafkaSpout<String, String> vicEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, vicEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, vicDataEventBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}

	
	protected void buildFloodForecastEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "FLOOD_FORECAST_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "FLOOD_FORECAST_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "FLOOD_FORECAST_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "FLOOD_FORECAST_EVENT";
		String zkRoot = "/flood_forecast_event";
		String zkSpoutId = "flood_forecast_event-spout";
		String zkBoltId = "flood_forecast_event-bolt";

		FloodForecastComputationBolt forecastCompBolt = new FloodForecastComputationBolt();
		KafkaSpout<String, String> vicEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, vicEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, forecastCompBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}


	protected void buildFloodForecastAggrEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "FLOOD_FORECAST_AGGR_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "FLOOD_FORECAST_AGGR_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "FLOOD_FORECAST_AGGR_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "FLOOD_FORECAST_AGGR_EVENT";
		String zkRoot = "/flood_forecast_aggr_event";
		String zkSpoutId = "flood_forecast_aggr_event-spout";
		String zkBoltId = "flood_forecast_aggr_event-bolt";

		FloodForecastAggregationBolt forecastAggrBolt = new FloodForecastAggregationBolt();
		KafkaSpout<String, String> vicEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, vicEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, forecastAggrBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}

	
	protected void buildCalibrationEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CALIBRATION_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CALIBRATION_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CALIBRATION_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "CALIBRATION_EVENT";
		String zkRoot = "/calibration_event";
		String zkSpoutId = "calibration_event-spout";
		String zkBoltId = "calibration_event-bolt";

		CalibrationBolt calibrationBolt = new CalibrationBolt();
		KafkaSpout<String, String> vicEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, vicEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, calibrationBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}
	
	
	protected void buildHecrasInputEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HECRAS_INPUT_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HECRAS_INPUT_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HECRAS_INPUT_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "HECRAS_INPUT_EVENT";
		String zkRoot = "/hecras_input_event";
		String zkSpoutId = "hecras_input_event-spout";
		String zkBoltId = "hecras_input_event-bolt";

		HecrasInputEventBolt hecrasBolt = new HecrasInputEventBolt();
		KafkaSpout<String, String> hecrasEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, hecrasEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, hecrasBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}

	
	protected void buildHecrasOutputEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HECRAS_OUTPUT_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HECRAS_OUTPUT_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HECRAS_OUTPUT_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "HECRAS_OUTPUT_EVENT";
		String zkRoot = "/hecras_output_event";
		String zkSpoutId = "hecras_output_event-spout";
		String zkBoltId = "hecras_output_event-bolt";

		HecrasOutputEventBolt hecrasBolt = new HecrasOutputEventBolt();
		KafkaSpout<String, String> hecrasEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, hecrasEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, hecrasBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}


	protected void buildHecrasAggrEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HECRAS_AGGR_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HECRAS_AGGR_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HECRAS_AGGR_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "HECRAS_AGGR_EVENT";
		String zkRoot = "/hecras_aggr_event";
		String zkSpoutId = "hecras_aggr_event-spout";
		String zkBoltId = "hecras_aggr_event-bolt";

		HecrasAggrBolt hecrasBolt = new HecrasAggrBolt();
		KafkaSpout<String, String> hecrasEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, hecrasEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, hecrasBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}


	protected void buildCatchmentFlowAggrEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CATCHMENT_FLOW_AGGR_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CATCHMENT_FLOW_AGGR_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "CATCHMENT_FLOW_AGGR_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "CATCHMENT_FLOW_AGGR_EVENT";
		String zkRoot = "/catchment_flow_aggr_event";
		String zkSpoutId = "catchment_flow_aggr_event-spout";
		String zkBoltId = "catchment_flow_aggr_event-bolt";

		CatchmentFlowAggrBolt hecrasBolt = new CatchmentFlowAggrBolt();
		KafkaSpout<String, String> hecrasEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, hecrasEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, hecrasBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}

	
	protected void buildHMSInputEvetnTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HMS_INPUT_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HMS_INPUT_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HMS_INPUT_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "HMS_INPUT_EVENT";
		String zkRoot = "/hms_input_event";
		String zkSpoutId = "hms_input_event-spout";
		String zkBoltId = "hms_input_event-bolt";

		HMSInputBolt hmsInputBolt = new HMSInputBolt();
		KafkaSpout<String, String> hmsEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, hmsEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, hmsInputBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}
	
	
	protected void buildHMSOutputEvetnTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HMS_OUTPUT_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HMS_OUTPUT_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HMS_OUTPUT_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "HMS_OUTPUT_EVENT";
		String zkRoot = "/hms_output_event";
		String zkSpoutId = "hms_output_event-spout";
		String zkBoltId = "hms_output_event-bolt";

		HMSOutputBolt hmsOutputBolt = new HMSOutputBolt();
		KafkaSpout<String, String> hmsEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, hmsEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, hmsOutputBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}

	
	protected void buildHMSAggregationEvetnTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HMS_AGGR_EVENT.NO_SPOUT_EXECUTORS", 1);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HMS_AGGR_EVENT.NO_BOLT_EXECUTORS", 1);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "HMS_AGGR_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "HMS_AGGR_EVENT";
		String zkRoot = "/hms_aggr_event";
		String zkSpoutId = "hms_aggr_event-spout";
		String zkBoltId = "hms_aggr_event-bolt";

		HMSAggrBolt hmsAggrBolt = new HMSAggrBolt();
		KafkaSpout<String, String> hmsEventSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, hmsEventSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, hmsAggrBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);
	}

	
	protected void buildTestDataEventTopologyPartial(TopologyBuilder builder, String topologyName) {
		int numSpoutExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "TEST_DATA_EVENT.NO_SPOUT_EXECUTORS", 2);
		int numBoltExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "TEST_DATA_EVENT.NO_BOLT_EXECUTORS", 4);
		int taskMulttiplier = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "TEST_DATA_EVENT.TASKS_MULTIPLIER", 1);

		String zkTopic = "TEST_DATA_EVENT";
		String zkRoot = "/test_data_event";
		String zkBoltId = "test_data_event-bolt";
		String zkSpoutId = "test_data_event-spout";

		TestDataEventBolt modelBolt = new TestDataEventBolt();
		KafkaSpout<String, String> rainfallKafkaSpout = buildKafkaSpout(zkTopic, zkRoot, zkSpoutId, topologyName);
		builder.setSpout(zkSpoutId, rainfallKafkaSpout, numSpoutExecutors).setNumTasks(numSpoutExecutors * taskMulttiplier);
		builder.setBolt(zkBoltId, modelBolt, numBoltExecutors).setNumTasks(numBoltExecutors * taskMulttiplier).shuffleGrouping(zkSpoutId);

	}
*/

	
	private KafkaSpout<String, String> buildKafkaSpout(String zkTopic, String zkRoot, String zkSpoutId, String topologyName) {
		String kafkaHosts = configPropertyRepoService.getProperty("KAFKA_CONFIG", "default_kafka", "KAFKA_HOST_PORT", "localhost:9092");
		
		KafkaSpout<String, String> kafkaSpout = new KafkaSpout<String, String>(KafkaSpoutConfig.builder(kafkaHosts, zkTopic)
				.setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)
				.setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)
				.setProp(ConsumerConfig.GROUP_ID_CONFIG, topologyName +"-" + zkSpoutId).build());
		
		return kafkaSpout;
	}

	
	private List<TopologyBuilderWrapper> buildATopologyWithAllSpoutsAndBoltsTogether(String topologyName) {
		List<TopologyBuilderWrapper> topologyBuilderWrapperList = new ArrayList<TopologyBuilderWrapper>();
		TopologyBuilder builder = new TopologyBuilder();

		buildCassAggrEventTopologyPartial(builder, topologyName);
		buildScraperEventTopologyPartial(builder, topologyName);
//		buildPumpHouseEventTopologyPartial(builder, topologyName);
		buildRainfallEventTopologyPartial(builder, topologyName);
//		buildTemperatureEventTopologyPartial(builder, topologyName);
//		buildHumidityEventTopologyPartial(builder, topologyName);
//		buildWindEventTopologyPartial(builder, topologyName);
//		buildPressureEventTopologyPartial(builder, topologyName);
//		buildRiverGaugeEventTopologyPartial(builder, topologyName);
//		buildReservoirEventTopologyPartial(builder, topologyName);
//		buildCanalEventTopologyPartial(builder, topologyName);
//		buildModelDataEventTopologyPartial(builder, topologyName);
//		buildICMPropagationEventTopologyPartial(builder, topologyName);

//		ENABLE FOR FLOOD FORECAST DEPLOYMENT
//		buildVICRoutEventTopologyPartial(builder, topologyName); 
//		buildVICInputEventTopologyPartial(builder, topologyName); 
//		buildFloodForecastEventTopologyPartial(builder, topologyName);
//		buildFloodForecastAggrEventTopologyPartial(builder, topologyName);
//		buildCalibrationEventTopologyPartial(builder, topologyName);
//		buildHecrasInputEventTopologyPartial(builder, topologyName);
//		buildHecrasOutputEventTopologyPartial(builder, topologyName);
//		buildHecrasAggrEventTopologyPartial(builder, topologyName);
//		buildCatchmentFlowAggrEventTopologyPartial(builder, topologyName);
//		buildHMSInputEvetnTopologyPartial(builder, topologyName);
//		buildHMSOutputEvetnTopologyPartial(builder, topologyName);
//		buildHMSAggregationEvetnTopologyPartial(builder, topologyName);
		
//		ENABLE FOR SATELLITE DATA PROCESSING
//		buildDownloadEventTopologyPartial(builder, topologyName);
//		buildProcessingEventTopologyPartial(builder, topologyName);

//		ENABLE FOR WEATHER FORECAST DOWNLOAD & AGGREGATION
//		buildForecastDownloadEventTopologyPartial(builder, topologyName);
//		buildForecastAggregationEventTopologyPartial(builder, topologyName);
//		buildCycloneDownloadEventTopologyPartial(builder, topologyName);
//		buildWeatherDataScrapingEventTopologyPartial(builder, topologyName);
//		buildNowcastScrapingEventTopologyPartial(builder, topologyName);
//		buildReportGenerationEventTopologyPartial(builder, topologyName);
		
// 		ENABLE FOR TESTING PURPOSE
//		buildTestDataEventTopologyPartial(builder, topologyName);
		TopologyBuilderWrapper topologyBuilderWrapper = new TopologyBuilderWrapper(topologyName, builder);
		topologyBuilderWrapperList.add(topologyBuilderWrapper);
		return topologyBuilderWrapperList;
	}



	public void buildAndSubmitTopologies() throws InvalidTopologyException {
		int workerProcesses = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "TOPOLOGY.WORKER_PROCESSES", 1);
		int ackExecutors = configPropertyRepoService.getIntProperty("STORM_CONFIG", "default_storm", "TOPOLOGY.ACKEXECUTORS", 5);
		String topologyName = configPropertyRepoService.getProperty("STORM_CONFIG", "default_storm", "TOPOLOGY.NAME", "event-topology");


		List<TopologyBuilderWrapper> topologyBuilderWrapperList = buildATopologyWithAllSpoutsAndBoltsTogether(topologyName);
		for (TopologyBuilderWrapper topologyBuilderWrapper : topologyBuilderWrapperList) {
			
			try {
				Config config = new Config();
				config.setNumAckers(ackExecutors);
				config.setNumWorkers(workerProcesses);

				StormSubmitter.submitTopology(topologyBuilderWrapper.getTopologyName(), config,
						topologyBuilderWrapper.getTopologyBuilder().createTopology());
				
			} catch (AlreadyAliveException | AuthorizationException e) {
				System.out.println("Topology is already alive : " + topologyBuilderWrapper 
						+ ", exception is " + e);
			}
		}
	}

	

	private class TopologyBuilderWrapper {
		private String topologyName;
		private TopologyBuilder topologyBuilder;

		public TopologyBuilderWrapper(String topologyName, TopologyBuilder topologyBuilder) {
			super();
			this.topologyName = topologyName;
			this.topologyBuilder = topologyBuilder;
		}

		public String getTopologyName() {
			return topologyName;
		}

		@SuppressWarnings("unused")
		public void setTopologyName(String topologyName) {
			this.topologyName = topologyName;
		}

		public TopologyBuilder getTopologyBuilder() {
			return topologyBuilder;
		}

		@SuppressWarnings("unused")
		public void setTopologyBuilder(TopologyBuilder topologyBuilder) {
			this.topologyBuilder = topologyBuilder;
		}

		@Override
		public String toString() {
			return "TopologyBuilderWrapper [topologyName=" + topologyName + ", topologyBuilder=" 
					+ topologyBuilder + "]";
		}
	}
	
	
	
	/*public static void main(String[] args) throws InvalidTopologyException, DSPException {
		System.out.println("Hello World!");

		@SuppressWarnings("resource")
		ApplicationContext context = new ClassPathXmlApplicationContext(
				new String[] { "spring/platform-application-context.xml", "spring/dsp-application-context.xml" });

		CreateTopology createTopology = context.getBean(CreateTopology.class);
		createTopology.buildAndSubmitTopologies();
	}*/


	
	public void test(ApplicationContext context) throws DSPException {}

}

