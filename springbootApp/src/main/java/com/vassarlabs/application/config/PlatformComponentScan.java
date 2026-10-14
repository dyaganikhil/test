package com.vassarlabs.application.config;


import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
        "com.vassarlabs.cassandra",
        "com.vassarlabs.boundarydetection",
        "com.vassarlabs.gisoperations",
        "com.vassarlabs.rawdata",
        "com.vassarlabs.eventmapper",
        "com.vassarlabs.eventmap",
        "com.vassarlabs.common",
        "com.vassarlabs.config",
        "com.vassarlabs.location",
        "com.vassarlabs.kstorm",
        "com.vassarlabs.iwm",
        "com.vassarlabs.icm",
        "com.vassarlabs.user",
        "com.vassarlabs.test",
        "com.vassarlabs.largeobject",
        "com.vassarlabs.deploy",
        "com.vassarlabs.usersession",
        "com.vassarlabs.eventmd",
        "com.vassarlabs.kafka",
        "com.vassarlabs.kflink",
        "com.vassarlabs.spatial",
        "com.vassarlabs.scrapers",
        "com.vassarlabs.monitorsystem",
        "com.vassarlabs.cass",
        "com.vassarlabs.taskmngr",
        "com.vassarlabs.userlogin",
        "com.vassarlabs.email",
        "com.vassarlabs.pictorialanalysis",
        "com.vassarlabs.platform.boundarydetection",
        "com.vassarlabs.cropanalysis",
        "com.vassarlabs.drought",
        "com.vassarlabs.sms",
        "com.vassarlabs.schedulermgmt",
        "com.vassarlabs.wbis",
        "com.vassarlabs.humidity",
        "com.vassarlabs.modelinventory",
        "com.vassarlabs.sdp",
        "com.vassarlabs.croplibrary",
        "com.vassarlabs.pestlibrary",
        "com.vassarlabs.notification",
        "com.vassarlabs.azurestorage",
        "com.vassarlabs.usermgmt",
        "com.vassarlabs.elibrary",
        "com.vassarlabs.soilhealthcard",
        "com.vassarlabs.alerts",
        "com.vassarlabs.elasticsearch",
        "com.vassarlabs.cassdata"
})
public class PlatformComponentScan {
}
