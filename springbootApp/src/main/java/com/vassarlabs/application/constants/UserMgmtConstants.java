package com.vassarlabs.application.constants;

import java.util.Map;
import java.util.HashMap;

public class UserMgmtConstants {

    public static final String IS_HEADER_REQUIRED = "isHeaderRequired";
    public static final String CONTENT_TYPE_LABEL = "Content-Type";
    public static final String IS_TRUE = "true";
    public static final String TOKEN_LABEL = "token";
    public static final String CSRF_TOKEN_LABEL = "Csrf-Token";
    public static final String AUTHORIZATION_HEADER = "Authorization";
    public static final String USER_ID_LABEL = "userId";
    public static final String LOCATION_AUTH_DATA_LABEL = "X-Loc-Auth-Data";
    public static final boolean IS_USERMGMT = false;
    public static final String USERMGMT_CONFIG_FILE_LOCATION = "file:/home/vassarlabs/config/wrims_usermgmt_application.yml";

    public static final Map<String, String> apiNamesToEndPointNamesMap = new HashMap<>();

    static {

        //Boundary detection APIs
        apiNamesToEndPointNamesMap.put("api/boundarydetection/testGetApi","/boundarydetection/testGetApi");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/config/componentVsAlgo","/boundarydetection/config/componentVsAlgo");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/config/sources","/boundarydetection/config/sources");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/polygon/user","/boundarydetection/polygon/user");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/polygon/geojson","/boundarydetection/polygon/geojson");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/polygon/geojson/delete","/boundarydetection/polygon/geojson/delete");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/polygon/shapefile","/boundarydetection/polygon/shapefile");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/grids/intersection","/boundarydetection/grids/intersection");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/polygon/computed","/boundarydetection/polygon/computed");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/polygon/computed/update","/boundarydetection/polygon/computed/update");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/polygon/computed/insertdelete","/boundarydetection/polygon/computed/insertdelete");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/train","/boundarydetection/train");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/predict","/boundarydetection/predict");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/retrain","/boundarydetection/retrain");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/user/model/intermediateOutput","/boundarydetection/user/model/intermediateOutput");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/user/model/results","/boundarydetection/user/model/results");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/taskmanager","/boundarydetection/taskmanager");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/aoi/delete","/boundarydetection/aoi/delete");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/taskmanager/restart","/boundarydetection/taskmanager/restart");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/taskmanager/deleteOutput","/boundarydetection/taskmanager/deleteOutput");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/taskmanager/cancel","/boundarydetection/taskmanager/cancel");
        apiNamesToEndPointNamesMap.put("api/locations/allChildLocationList","/locations/allChildLocationList");
        apiNamesToEndPointNamesMap.put("api/locations/allLocationList","/locations/allLocationList");
        apiNamesToEndPointNamesMap.put("api/locations/deleteStations","/locations/deleteStations");
        apiNamesToEndPointNamesMap.put("api/location/GeoJsonWithinLoc","/location/GeoJsonWithinLoc");
        apiNamesToEndPointNamesMap.put("api/locations/allChildrenForParentChildType","/locations/allChildrenForParentChildType");
        apiNamesToEndPointNamesMap.put("api/locations/heirarchy","/locations/heirarchy");
        apiNamesToEndPointNamesMap.put("api/locations/abbrlocations","/locations/abbrlocations");
        apiNamesToEndPointNamesMap.put("api/locations/getChildLocations","/locations/getChildLocations");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/upload/fbmetadata","/boundarydetection/upload/fbmetadata");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/polygon/metadata","/boundarydetection/polygon/metadata");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/farm/geojson","/boundarydetection/farm/geojson");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/fetchUpdatedBoundariesByPerformingGeometryOperations","/boundarydetection/fetchUpdatedBoundariesByPerformingGeometryOperations");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/insertUpdatedBoundaries","/boundarydetection/insertUpdatedBoundaries");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/insertfarmpolygon","/boundarydetection/insertfarmpolygon");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/deleteFarm","/boundarydetection/deleteFarm");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/getFieldDetails","/boundarydetection/getFieldDetails");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/getFarmerAndFieldDetails","/boundarydetection/getFarmerAndFieldDetails");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/updateFarmerDetails","/boundarydetection/updateFarmerDetails");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/modifyPredictedBoundaries","/boundarydetection/modifyPredictedBoundaries");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/modifyExistingBoundaries","/boundarydetection/modifyExistingBoundaries");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/submitUserBaseMap","/boundarydetection/submitUserBaseMap");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/fetchUserUploadedBaseMaps","/boundarydetection/fetchUserUploadedBaseMaps");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/performInterpolatePolygons","/boundarydetection/performInterpolatePolygons");
        apiNamesToEndPointNamesMap.put("/api/boundarydetection/deleteUserBaseMap", "/boundarydetection/deleteUserBaseMap");

        //Location Apis
        apiNamesToEndPointNamesMap.put("api/locations/allParentAndChildLocationList","/locations/allParentAndChildLocationList");
        apiNamesToEndPointNamesMap.put("/api/locations/insertLocationExtension", "/locations/insertLocationExtension");
        apiNamesToEndPointNamesMap.put("/api/locations/getLocationExtensions", "/locations/getLocationExtensions");
        apiNamesToEndPointNamesMap.put("/api/locations/insertLocationType", "/locations/insertLocationType");




        //Pictorial Analysis Apis
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/testGetApi","/pictorialanalysis/testGetApi");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/submitLabeledImages","/pictorialanalysis/submitLabeledImages");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/getImages","/pictorialanalysis/getImages");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/submitRawImage","/pictorialanalysis/submitRawImage");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/runPredictionOnTrainedImages","/pictorialanalysis/runPredictionOnTrainedImages");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/predict","/pictorialanalysis/predict");
        apiNamesToEndPointNamesMap.put("api/pictorialAnalysis/getImagesForTrainingOrPrediction","/pictorialAnalysis/getImagesForTrainingOrPrediction");
        apiNamesToEndPointNamesMap.put("api/pictorialAnalysis/config/algorithms","/pictorialAnalysis/config/algorithms");
        apiNamesToEndPointNamesMap.put("api/pictorialAnalysis/trainOrRetrain","/pictorialAnalysis/trainOrRetrain");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/getpredictedimages","/pictorialanalysis/getpredictedimages");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/getapplicationdetails","/pictorialanalysis/getapplicationdetails");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/uploadRawImages","/pictorialanalysis/uploadRawImages");
        apiNamesToEndPointNamesMap.put("api/pictorialAnalysis/getUserTasks","/pictorialAnalysis/getUserTasks");
        apiNamesToEndPointNamesMap.put("api/pictorialAnalysis/getFarmRawImages","/pictorialAnalysis/getFarmRawImages");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/submitRawImages","/pictorialanalysis/submitRawImages");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/deleteRawImages","/pictorialanalysis/deleteRawImages");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/getImageUploadDistribution","/pictorialanalysis/getImageUploadDistribution");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/getConfusionMatrix","/pictorialanalysis/getConfusionMatrix");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/submitTempRawImages","/pictorialanalysis/submitTempRawImages");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/getTempFarmRawImages","/pictorialanalysis/getTempFarmRawImages");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/getCSVTemplateForImageUpload","/pictorialanalysis/getCSVTemplateForImageUpload");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/getComponentVsDropdowns","/pictorialanalysis/getComponentVsDropdowns");
        apiNamesToEndPointNamesMap.put("api/locations/allParentAndChildLocationOfATypeList","/locations/allParentAndChildLocationOfATypeList");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/getLocationsFromLocTypeMD","/pictorialanalysis/getLocationsFromLocTypeMD");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/getRawImageDetails","/pictorialanalysis/getRawImageDetails");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/updateRawImage","/pictorialanalysis/updateRawImage");
        apiNamesToEndPointNamesMap.put("api/pictorialAnalysis/objectDetection/insertNewObjectLabel","/pictorialAnalysis/objectDetection/insertNewObjectLabel");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/submitFarmerImage","/pictorialanalysis/submitFarmerImage");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/getFarmerImage","/pictorialanalysis/getFarmerImage");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/submitPlotImages","/pictorialanalysis/submitPlotImages");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/getPlotImages","/pictorialanalysis/getPlotImages");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/predictionOnDemand","/pictorialanalysis/predictionOnDemand");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/pest/masterData","/pictorialanalysis/pest/masterData");
        apiNamesToEndPointNamesMap.put("api/pictorialanalysis/historyData","/pictorialanalysis/historyData");
        apiNamesToEndPointNamesMap.put("api/pictorialAnalysis/getFarmImages","/pictorialAnalysis/getFarmImages");

        //Crop Analysis Apis
        apiNamesToEndPointNamesMap.put("api/cropanalysis/testGetApi","/cropanalysis/testGetApi");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/submitCropAndOwnerInfo","/cropanalysis/submitCropAndOwnerInfo");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/aggregateCropData","/cropanalysis/aggregateCropData");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/metadata/getFarmLevelData","/cropanalysis/metadata/getFarmLevelData");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/metadata/aggregatedCropMetaData","/cropanalysis/metadata/aggregatedCropMetaData");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/getSeasonDetails","/cropanalysis/getSeasonDetails");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/getCropYearAndCropSeason","/cropanalysis/getCropYearAndCropSeason");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/getSeasonStartAndEndTS","/cropanalysis/getSeasonStartAndEndTS");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/getPreviousSeason","/cropanalysis/getPreviousSeason");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/fetchAllCropDetails","/cropanalysis/fetchAllCropDetails");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/keralaMobileApp/setFarmerToFieldMapping","/cropAnalysis/keralaMobileApp/setFarmerToFieldMapping");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/keralaMobileApp/submitfarmersdetails","/cropAnalysis/keralaMobileApp/submitfarmersdetails");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/keralaMobileApp/getFarmerDetails","/cropAnalysis/keralaMobileApp/getFarmerDetails");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/keralaMobileApp/getAllFieldDetailsForVillage","/cropAnalysis/keralaMobileApp/getAllFieldDetailsForVillage");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/submitCropAndOwnerInfoBySurveyor","/cropAnalysis/submitCropAndOwnerInfoBySurveyor");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/getFarmLevelDataForSurveyor","/cropAnalysis/getFarmLevelDataForSurveyor");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/submitFarmLevelInfoByFarmer","/cropAnalysis/submitFarmLevelInfoByFarmer");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/getFarmLevelCropDataForFarmer","/cropAnalysis/getFarmLevelCropDataForFarmer");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/submitCropAndOwnerInfoForPoint","/cropanalysis/submitCropAndOwnerInfoForPoint");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/metadata/getFarmLevelDataForPoint","/cropanalysis/metadata/getFarmLevelDataForPoint");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/getStateSeasonsVsLanguage","/cropanalysis/getStateSeasonsVsLanguage");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/farmerAndCropData", "/cropAnalysis/farmerAndCropData");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/keralaMobileApp/getAllFieldDetailsForFarmer", "/cropanalysis/keralaMobileApp/getAllFieldDetailsForFarmer");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/keralaMobileApp/deleteFarmer", "/cropanalysis/keralaMobileApp/deleteFarmer");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/deleteActualCropSownAreaSubmittedByDepartmentBO", "/cropanalysis/deleteActualCropSownAreaSubmittedByDepartmentBO");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/approvalOfCropSownDataByBO", "/cropanalysis/approvalOfCropSownDataByBO");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/getAllFarmersData", "/cropanalysis/getAllFarmersData");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/updateFarmerStatus", "/cropanalysis/updateFarmerStatus");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/updateFieldStatus", "/cropanalysis/updateFieldStatus");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/getAllFieldDetailsForVillages", "/cropanalysis/getAllFieldDetailsForVillages");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/chatbot/farmerdata", "/cropanalysis/chatbot/farmerdata");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/parseAndInsertCropDataFarmerData", "/cropanalysis/parseAndInsertCropDataFarmerData");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/getFarmerCount", "/cropAnalysis/getFarmerCount");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/gtMobileApp/harmonization", "/cropanalysis/gtMobileApp/harmonization");
        apiNamesToEndPointNamesMap.put("api/cropSownAnalysis/getCropsHistoricalData","/cropSownAnalysis/getCropsHistoricalData");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/parseAndUpdateFarmerLocationData", "/cropAnalysis/parseAndUpdateFarmerLocationData");
        apiNamesToEndPointNamesMap.put("api/cropanalysis/fetchAllCropDetailsForCropCategory","/cropanalysis/fetchAllCropDetailsForCropCategory");
        apiNamesToEndPointNamesMap.put("api/cropSownAnalysis/getLocationCropsData","/cropSownAnalysis/getLocationCropsData");

        //Content Management Apis
        apiNamesToEndPointNamesMap.put("api/boundarydetection/upload/tempfbmetadata","/boundarydetection/upload/tempfbmetadata");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/submitProcessedPolygons","/boundarydetection/submitProcessedPolygons");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/contentmgmnt/getCountForFarmProcessedEntitiesForAllLocType","/boundarydetection/contentmgmnt/getCountForFarmProcessedEntitiesForAllLocType");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/contentmgmnt/getFarmProcessedEntityUUIDsList","/boundarydetection/contentmgmnt/getFarmProcessedEntityUUIDsList");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/contentmgmnt/deleteTask","/boundarydetection/contentmgmnt/deleteTask");
        apiNamesToEndPointNamesMap.put("api/boundarydetection/contentmgmnt/cancelTask","/boundarydetection/contentmgmnt/cancelTask");

        //Crop Growth Apis
        apiNamesToEndPointNamesMap.put("api/cropGrowthAnalysis/getCropGrowthIndicesData","/cropGrowthAnalysis/getCropGrowthIndicesData");
        apiNamesToEndPointNamesMap.put("api/cropGrowthAnalysis/getCropGrowthIndicesByStartAndEndTs","/cropGrowthAnalysis/getCropGrowthIndicesByStartAndEndTs");
        apiNamesToEndPointNamesMap.put("api/cropGrowthAnalysis/submitAoi","/cropGrowthAnalysis/submitAoi");
        apiNamesToEndPointNamesMap.put("api/cropGrowthAnalysis/getSavedAOI","/cropGrowthAnalysis/getSavedAOI");
        apiNamesToEndPointNamesMap.put("api/cropGrowthAnalysis/submitRequestForCropGrowthIndicesForUserDefinedArea","/cropGrowthAnalysis/submitRequestForCropGrowthIndicesForUserDefinedArea");
        apiNamesToEndPointNamesMap.put("api/cropGrowthAnalysis/getCropGrowthIndicesDataForAoiByStartAndEndDate","/cropGrowthAnalysis/getCropGrowthIndicesDataForAoiByStartAndEndDate");
        apiNamesToEndPointNamesMap.put("api/cropGrowthAnalysis/getCropGrowthImagesDataForCropYearAndSeason","/cropGrowthAnalysis/getCropGrowthImagesDataForCropYearAndSeason");
        apiNamesToEndPointNamesMap.put("api/cropGrowthAnalysis/getCropGrowthImagesByStartAndEndTs","/cropGrowthAnalysis/getCropGrowthImagesByStartAndEndTs");
        apiNamesToEndPointNamesMap.put("api/cropGrowthAnalysis/getUserTasks","/cropGrowthAnalysis/getUserTasks");
        apiNamesToEndPointNamesMap.put("api/cropGrowthAnalysis/getCropGrowthIndicesDataForAoiByCropYearAndSeason","/cropGrowthAnalysis/getCropGrowthIndicesDataForAoiByCropYearAndSeason");
        apiNamesToEndPointNamesMap.put("api/cropGrowthAnalysis/deleteSavedAoi","/cropGrowthAnalysis/deleteSavedAoi");
        apiNamesToEndPointNamesMap.put("api/cropGrowthAnalysis/getCropGrowthIndicesForUserDefinedArea","/cropGrowthAnalysis/getCropGrowthIndicesForUserDefinedArea");


        //SDP Apis
        apiNamesToEndPointNamesMap.put("api/sdp/testGetApi","/sdp/testGetApi");
        apiNamesToEndPointNamesMap.put("api/sdp/satellite/config","/sdp/satellite/config");
        apiNamesToEndPointNamesMap.put("api/sdp/polygon/user","/sdp/polygon/user");
        apiNamesToEndPointNamesMap.put("api/sdp/polygon/geojson","/sdp/polygon/geojson");
        apiNamesToEndPointNamesMap.put("api/sdp/polygon/shapefile","/sdp/polygon/shapefile");
        apiNamesToEndPointNamesMap.put("api/sdp/satellite/preview","/sdp/satellite/preview");
        apiNamesToEndPointNamesMap.put("api/sdp/satellite/processing/preview","/sdp/satellite/processing/preview");
        apiNamesToEndPointNamesMap.put("api/sdp/satellite/acquire","/sdp/satellite/acquire");
        apiNamesToEndPointNamesMap.put("api/sdp/satellite/process","/sdp/satellite/process");
        apiNamesToEndPointNamesMap.put("api/sdp/cart/insertOrUpdateCartData","/sdp/cart/insertOrUpdateCartData");
        apiNamesToEndPointNamesMap.put("api/sdp/cart/getdata","/sdp/cart/getdata");
        apiNamesToEndPointNamesMap.put("api/sdp/cart/delete","/sdp/cart/delete");
        apiNamesToEndPointNamesMap.put("api/sdp/taskmanager","/sdp/taskmanager");
        apiNamesToEndPointNamesMap.put("api/sdp/aoi/delete","/sdp/aoi/delete");
        apiNamesToEndPointNamesMap.put("api/sdp/search","/sdp/search");
        apiNamesToEndPointNamesMap.put("api/sdp/process","/sdp/process");
        apiNamesToEndPointNamesMap.put("api/sdp/download","/sdp/download");
        apiNamesToEndPointNamesMap.put("api/sdp/availableIndicies","/sdp/availableIndicies");
        apiNamesToEndPointNamesMap.put("api/sdp/satellite/configmap","/sdp/satellite/configmap");
        apiNamesToEndPointNamesMap.put("api/sdp/cart/save","/sdp/cart/save");
        apiNamesToEndPointNamesMap.put("api/sdp/cart/getSatelliteSearchCartItems","/sdp/cart/getSatelliteSearchCartItems");
        apiNamesToEndPointNamesMap.put("api/taskmanagement/assign","/taskmanagement/assign");
        apiNamesToEndPointNamesMap.put("api/taskmanagement/acknowledge","/taskmanagement/acknowledge");
        apiNamesToEndPointNamesMap.put("api/sdp/cart/deleteCartItem","/sdp/cart/deleteCartItem");
        apiNamesToEndPointNamesMap.put("api/sdp/cart/getCartItem","/sdp/cart/getCartItem");
        apiNamesToEndPointNamesMap.put("api/sdp/downloadlocal","/sdp/downloadlocal");
        apiNamesToEndPointNamesMap.put("api/sdp/insertOrUpdateSDPConfigMap","/sdp/insertOrUpdateSDPConfigMap");
        apiNamesToEndPointNamesMap.put("api/sdp/getSatelliteIndices","/sdp/getSatelliteIndices");
        apiNamesToEndPointNamesMap.put("api/sdp/satellite/config/data","/sdp/satellite/config/data");
        apiNamesToEndPointNamesMap.put("api/sdp/delete","/sdp/delete");
        apiNamesToEndPointNamesMap.put("api/sdp/aggregate/indices","/sdp/aggregate/indices");
        apiNamesToEndPointNamesMap.put("api/sdp/cancelQueueTask","/sdp/cancelQueueTask");
        apiNamesToEndPointNamesMap.put("api/sdp/deleteTask","/sdp/deleteTask");
        apiNamesToEndPointNamesMap.put("api/sdp/restartTask","/sdp/restartTask");
        apiNamesToEndPointNamesMap.put("api/sdp/downloadflink","/sdp/downloadflink");
        apiNamesToEndPointNamesMap.put("api/sdp/getOfflineSatelliteData","/sdp/getOfflineSatelliteData");
        apiNamesToEndPointNamesMap.put("api/sdp/insertPredictedDataAsCropSownData","/sdp/insertPredictedDataAsCropSownData");
        apiNamesToEndPointNamesMap.put("api/sdp/products/files","/sdp/products/files");


        //SDP supervised Crop classification APIs
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/config/algorithms","/sdp/cropClassification/config/algorithms");
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/fetchTrainingSampleData","/sdp/cropClassification/fetchTrainingSampleData");
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/predict","/sdp/cropClassification/predict");
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/trainOrRetrain","/sdp/cropClassification/trainOrRetrain");
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/fetchUserModels","/sdp/cropClassification/fetchUserModels");
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/createGeoserverLayer","/sdp/cropClassification/createGeoserverLayer");
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/getSatelliteConfigMap","/sdp/cropClassification/getSatelliteConfigMap");
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification","/sdp/cropClassification");
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/supervised/deleteTask", "/sdp/cropClassification/supervised/deleteTask");

        //Drought APIs
        apiNamesToEndPointNamesMap.put("api/drought/rainfall/timeseries","/drought/rainfall/timeseries");
        apiNamesToEndPointNamesMap.put("api/drought/combined/timeseries","/drought/combined/timeseries");
        apiNamesToEndPointNamesMap.put("api/drought/getAvailablePeriodsForWindow","/drought/getAvailablePeriodsForWindow");

        //SDP Unsupervised crop classification
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/unsupervised/predict","/sdp/cropClassification/unsupervised/predict");
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/unsupervised/getClassTrendData","/sdp/cropClassification/unsupervised/getClassTrendData");
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/unsupervised/getCropTrendData","/sdp/cropClassification/unsupervised/getCropTrendData");
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/unsupervised/fetchAllCropDetails","/sdp/cropClassification/unsupervised/fetchAllCropDetails");
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/unsupervised/updatePredictionVectorData","/sdp/cropClassification/unsupervised/updatePredictionVectorData");
        apiNamesToEndPointNamesMap.put("/sdp/cropClassification/unsupervised/deleteTask","/sdp/cropClassification/unsupervised/deleteTask");
        apiNamesToEndPointNamesMap.put("/sdp/cropClassification/unsupervised/updateRasterStyle", "/sdp/cropClassification/unsupervised/updateRasterStyle");
        apiNamesToEndPointNamesMap.put("/sdp/cropClassification/unsupervised/deleteTempRasterStyle", "/sdp/cropClassification/unsupervised/deleteTempRasterStyle");
        apiNamesToEndPointNamesMap.put("sdp/cropClassification/unsupervised/getModelParams", "sdp/cropClassification/unsupervised/getModelParams");
        apiNamesToEndPointNamesMap.put("/sdp/cropClassification/unsupervised/updateRasterData", "/sdp/cropClassification/unsupervised/updateRasterData");
        apiNamesToEndPointNamesMap.put("/sdp/cropClassification/unsupervised/getClassVsColorMapping", "/sdp/cropClassification/unsupervised/getClassVsColorMapping");
        apiNamesToEndPointNamesMap.put("api/sdp/cropClassification/unsupervised/cropNameVsArea","/sdp/cropClassification/unsupervised/cropNameVsArea");


        // Scheduler APIs
        apiNamesToEndPointNamesMap.put("/schedulermgmt/job/insertOrUpdateScheduler", "/schedulermgmt/job/insertOrUpdateScheduler");
        apiNamesToEndPointNamesMap.put("/schedulermgmt/fetchAllJobs", "/schedulermgmt/fetchAllJobs");
        apiNamesToEndPointNamesMap.put("/schedulermgmt/stopOrResume", "/schedulermgmt/stopOrResume");
        apiNamesToEndPointNamesMap.put("/schedulermgmt/delete", "/schedulermgmt/delete");


        //Crop Harvest APIS
        apiNamesToEndPointNamesMap.put("api/cropHarvest/getCropHarvestData","/cropHarvest/getCropHarvestData");
        apiNamesToEndPointNamesMap.put("api/cropHarvest/parseAndInsert","/cropHarvest/parseAndInsert");
        apiNamesToEndPointNamesMap.put("api/cropHarvest/aggregateCropHarvestData","/cropHarvest/aggregateCropHarvestData");

        //Crop Sown Analysis APIs
        apiNamesToEndPointNamesMap.put("api/cropSownAnalysis/getCropSownMetaData","/cropSownAnalysis/getCropSownMetaData");
        apiNamesToEndPointNamesMap.put("api/cropSownAnalysis/getSources","/cropSownAnalysis/getSources");
        apiNamesToEndPointNamesMap.put("api/cropSownAnalysis/triggerNormalData","/cropSownAnalysis/triggerNormalData");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/submitActualCropSownAreaByDepartment","/cropAnalysis/submitActualCropSownAreaByDepartment");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/getCropSownAnalysisDataForDepartment","/cropAnalysis/getCropSownAnalysisDataForDepartment");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/deleteActualCropSownAreaSubmittedByDepartment","/cropAnalysis/deleteActualCropSownAreaSubmittedByDepartment");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/getCropSownAnalysisDataForDepartmentBO","/cropAnalysis/getCropSownAnalysisDataForDepartmentBO");
        apiNamesToEndPointNamesMap.put("api/cropSownAnalysis/parseAndInsertData","/cropSownAnalysis/parseAndInsertData");
        apiNamesToEndPointNamesMap.put("api/cropSownAnalysis/getCropConfig","/cropSownAnalysis/getCropConfig");
        apiNamesToEndPointNamesMap.put("api/cropSownAnalysis/parseAndInsertLandUtilizationData", "/cropSownAnalysis/parseAndInsertLandUtilizationData");
        apiNamesToEndPointNamesMap.put("api/cropSownAnalysis/getLandUtilizationData", "/cropSownAnalysis/getLandUtilizationData");
        apiNamesToEndPointNamesMap.put("api/cropSownAnalysis/getCropSownMetaDataForSat", "/cropSownAnalysis/getCropSownMetaDataForSat");
        apiNamesToEndPointNamesMap.put("api/getThematicLayerStatsByLocAndLayerType/","/getThematicLayerStatsByLocAndLayerType");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/getCropSownAnalysisDataFoMobileApp","/cropAnalysis/getCropSownAnalysisDataFoMobileApp");

        //Crop Content Management
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/addEditCropCategory","/cropContentManagement/addEditCropCategory");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/getCropCategory","/cropContentManagement/getCropCategory");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/getCropCategories","/cropContentManagement/getCropCategories");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/getFarmingTypes","/cropContentManagement/getFarmingTypes");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/addOrEditCropNameData","/cropContentManagement/addOrEditCropNameData");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/getStateList","/cropContentManagement/getStateList");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/getCropNames","/cropContentManagement/getCropNames");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/getCropVarietyMetaData","/cropContentManagement/getCropVarietyMetaData");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/getAllCrops","/cropContentManagement/getAllCrops");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/addEditCropVariety","/cropContentManagement/addEditCropVariety");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/getCropVarietyData","/cropContentManagement/getCropVarietyData");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/getSeasonList","/cropContentManagement/getSeasonList");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/uploadExcelData","/cropContentManagement/uploadExcelData");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/getTemplate","/cropContentManagement/getTemplate");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/uploadCropVarietyData","/cropContentManagement/uploadCropVarietyData");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/postgresAddEditCropCategory","/cropContentManagement/postgresAddEditCropCategory");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/postgresCropCategory","/cropContentManagement/postgresCropCategory");
        apiNamesToEndPointNamesMap.put("api/cropContentManagement/cropHierarchy","/cropContentManagement/cropHierarchy");


        // Crop Library APIs
        apiNamesToEndPointNamesMap.put("api/cropLibrary/testGetApi","/cropLibrary/testGetApi");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/insertOrUpdateCropGrowthStageData","/cropLibrary/insertOrUpdateCropGrowthStageData");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/getCropGrowthStageData","/cropLibrary/getCropGrowthStageData");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/addOrEditFarmingTypeData","/cropLibrary/addOrEditFarmingTypeData");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/getFarmingTypeData","/cropLibrary/getFarmingTypeData");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/addOrEditCropNameData","/cropLibrary/addOrEditCropNameData");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/getCropNameData","/cropLibrary/getCropNameData");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/addEditCropCategory","/cropLibrary/addEditCropCategory");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/getCropCategory","/cropLibrary/getCropCategory");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/addEditCropVariety","/cropLibrary/addEditCropVariety");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/getCropVarietyData","/cropLibrary/getCropVarietyData");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/insertOrUpdateCropAczFormData","/cropLibrary/insertOrUpdateCropAczFormData");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/fetchCropAczInfo","/cropLibrary/fetchCropAczInfo");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/downloadTemplate","/cropLibrary/downloadTemplate");
        apiNamesToEndPointNamesMap.put("api/cropLibrary/uploadCropData", "/cropLibrary/uploadCropData");


        // Pest Library APIs
        apiNamesToEndPointNamesMap.put("api/pestLibrary/testGetApi","/pestLibrary/testGetApi");
        apiNamesToEndPointNamesMap.put("api/pestLibrary/insertPestDiseaseMD","/pestLibrary/insertPestDiseaseMD");
        apiNamesToEndPointNamesMap.put("api/pestLibrary/insertOrUpdatePestDiseaseCropCongenialConditions","/pestLibrary/insertOrUpdatePestDiseaseCropCongenialConditions");
        apiNamesToEndPointNamesMap.put("api/pestLibrary/fetchPestDiseaseMDData","/pestLibrary/fetchPestDiseaseMDData");
        apiNamesToEndPointNamesMap.put("api/pestLibrary/fetchPestDiseaseCongenialData","/pestLibrary/fetchPestDiseaseCongenialData");
        apiNamesToEndPointNamesMap.put("api/pestLibrary/downloadPestDataTemplate","/pestLibrary/downloadPestDataTemplate");
        apiNamesToEndPointNamesMap.put("api/pestLibrary/uploadPestData","/pestLibrary/uploadPestData");


        // Model Inventory
        apiNamesToEndPointNamesMap.put("api/modelInventory/getModelsForUser", "/modelInventory/getModelsForUser");
        apiNamesToEndPointNamesMap.put("api/modelInventory/getModelsForSuperUser", "/modelInventory/getModelsForSuperUser");
        apiNamesToEndPointNamesMap.put("api/modelInventory/updateModelMetadata", "/modelInventory/updateModelMetadata");
        apiNamesToEndPointNamesMap.put("api/modelInventory/downloadModel", "/modelInventory/downloadModel");
        apiNamesToEndPointNamesMap.put("api/modelInventory/assignOrUnassignDefaultModel","/modelInventory/assignOrUnassignDefaultModel");
        apiNamesToEndPointNamesMap.put("api/modelInventory/assignOrUnassignDefaultModelToComponent","/modelInventory/assignOrUnassignDefaultModelToComponent");
        apiNamesToEndPointNamesMap.put("api/modelInventory/uploadModel", "/modelInventory/uploadModel");
        apiNamesToEndPointNamesMap.put("api/modelInventory/assignOrUnassignModelToUser", "/modelInventory/assignOrUnassignModelToUser");
        apiNamesToEndPointNamesMap.put("api/modelInventory/getapplicationdetails","/modelInventory/getapplicationdetails");

        // Weather Forecast
        apiNamesToEndPointNamesMap.put("api/api/weatherForecastv2/getChartData", "/api/weatherForecastv2/getChartData");
        apiNamesToEndPointNamesMap.put("api/api/weatherForecastv2/getTableData", "/api/weatherForecastv2/getTableData");
        apiNamesToEndPointNamesMap.put("api/api/weatherForecastv2/getHeatMapData", "/api/weatherForecastv2/getHeatMapData");
        apiNamesToEndPointNamesMap.put("api/api/weatherForecastv2/getDataForLatLon", "/api/weatherForecastv2/getDataForLatLon");
        apiNamesToEndPointNamesMap.put("api/api/weatherForecastv2/getForecastFiles","api/weatherForecastv2/getForecastFiles");
        apiNamesToEndPointNamesMap.put("api/api/weatherForecastv2/getEnsembleForecastData","api/weatherForecastv2/getEnsembleForecastData");
        apiNamesToEndPointNamesMap.put("api/api/weatherForecastv2/getEnsembleForecastHeatMap","api/weatherForecastv2/getEnsembleForecastHeatMap");
        apiNamesToEndPointNamesMap.put("api/api/weatherForecastv2/getFilteredTableData","api/weatherForecastv2/getFilteredTableData");
        apiNamesToEndPointNamesMap.put("api/api/weatherForecastv2/getExtendedHeatMapData","api/weatherForecastv2/getExtendedHeatMapData");


        //Soil Information APIs
        apiNamesToEndPointNamesMap.put("api/shc/getNutrientDataForAllChildLocations","/shc/getNutrientDataForAllChildLocations");

        //GT Dashboard
        apiNamesToEndPointNamesMap.put("api/sdp/getIndicesScaleFactorValues","/sdp/getIndicesScaleFactorValues");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/gtDashboard/getGTCropCountData","/cropAnalysis/gtDashboard/getGTCropCountData");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/gtDashboard/getIndicesDataForFields","/cropAnalysis/gtDashboard/getIndicesDataForFields");
        apiNamesToEndPointNamesMap.put("api/cropAnalysis/gtDashboard/getMinMaxIndicesdataOfFields","/cropAnalysis/gtDashboard/getMinMaxIndicesdataOfFields");


        //MICADA APIS
        apiNamesToEndPointNamesMap.put("api/api/rf/getAggregatedWeeklyData","api/rf/getAggregatedWeeklyData");
        apiNamesToEndPointNamesMap.put("api/api/aw/temperature/getAggregatedWeeklyData","api/aw/temperature/getAggregatedWeeklyData");
        apiNamesToEndPointNamesMap.put("api/api/crop/pest/masterdata/summary/root","api/crop/pest/masterdata/summary/root");
        apiNamesToEndPointNamesMap.put("api/api/crop/pest/percentage","api/crop/pest/percentage");
        apiNamesToEndPointNamesMap.put("api/api/pest/pestVsCropData","api/pest/pestVsCropData");
        apiNamesToEndPointNamesMap.put("api/api/v2/crop/pest/stress/location","api/v2/crop/pest/stress/location");
        apiNamesToEndPointNamesMap.put("api/api/crop/pest/stress/lastknowndata/root","api/crop/pest/stress/lastknowndata/root");
        apiNamesToEndPointNamesMap.put("api/api/crop/pest/stress/lastknowndata","api/crop/pest/stress/lastknowndata");
        apiNamesToEndPointNamesMap.put("api/api/v2/crop/pest/summary/stress","api/v2/crop/pest/summary/stress");
        apiNamesToEndPointNamesMap.put("api/api/v2/crop/pest/stress","api/v2/crop/pest/stress");
        apiNamesToEndPointNamesMap.put("api/api/v2/crop/masterdata/stages","api/v2/crop/masterdata/stages");
        apiNamesToEndPointNamesMap.put("api/api/v2/crop/masterdata","api/v2/crop/masterdata");
        apiNamesToEndPointNamesMap.put("api/api/crop/pest/masterdata","api/crop/pest/masterdata");
        apiNamesToEndPointNamesMap.put("api/crop/pest/stress/lastknowndata/getFarmStressDataFarmerWise","api/crop/pest/stress/lastknowndata/getFarmStressDataFarmerWise");
        apiNamesToEndPointNamesMap.put("api/api/crop/stress/ComponentVsData","api/crop/stress/ComponentVsData");
        apiNamesToEndPointNamesMap.put("api/api/crop/stress/ComponentVsFarmData","api/crop/stress/ComponentVsFarmData");

    }
}
