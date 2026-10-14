package com.vassarlabs.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;


// NOTE :: business module having iwm and icm should not be included here as its already mentioned in platform component scan
@Configuration
@ComponentScan(basePackages = {
    "com.vassarlabs.pictorialanalysis",
        "com.vassarlabs.cropanalysis",
        "com.vassarlabs.farmanalytics",
        "com.vassarlabs.mea",
        "com.vassarlabs.schemes",
        "com.vassarlabs.surveymgmt",
        "com.vassarlabs.soilhealthcard",
        "com.vassarlabs.humidity",
        "com.vassarlabs.wbis",
        "com.vassarlabs.farmmechanization",
        "com.vassarlabs.croploss",
        "com.vassarlabs.qualitycontrol",
        "com.vassarlabs.gt",
        "com.vassarlabs.sas",
        "com.vassarlabs.farmeractivity",
        "com.vassarlabs.riskanalysis",
        "com.vassarlabs.finance"
})
public class BusinessComponentScan {
}
