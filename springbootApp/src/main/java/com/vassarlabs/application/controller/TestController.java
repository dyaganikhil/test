package com.vassarlabs.application.controller;


import com.vassarlabs.common.dsp.context.DataStoreContext;
import com.vassarlabs.common.dsp.context.DbNode;
import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.dsp.rdbms.api.IRDBMSDataStore;
import com.vassarlabs.config.spring.AppContext;
import com.vassarlabs.common.logging.api.IVLLogger;
import com.vassarlabs.common.logging.api.IVLLoggerFactory;
import com.vassarlabs.common.logging.impl.VLLoggerFactory;
import com.vassarlabs.config.impl.APIResponse;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.*;
import org.apache.poi.xssf.usermodel.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

//import javax.imageio.ImageIO;
import jakarta.annotation.PostConstruct;
import jakarta.servlet.http.HttpServletRequest;
//import java.awt.image.BufferedImage;
//import java.io.File;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RestController
public class TestController {


    IVLLogger logger;

    @Autowired
    IVLLoggerFactory loggerFactory;

    @PostConstruct
    public void initLogger() {
        this.logger = loggerFactory.getLogger(getClass());
    }



//    @GetMapping("/testGetApi")
//    public APIResponse testGetAPI(){
//
//        APIResponse result = new APIResponse();
//        result.setMessage("hello!");
//        result.setResponse("Hi!");
//        result.setStatusCode(200);
//
////        getImagesForUser(1724092200000l, 1726770600000l, "shubham");
//
//        return result;
//    }


//    @PostMapping("generate-qrcode")
//    public ResponseEntity<String> testQrcodeGeneration(@RequestBody JsonNode postData) {
//        String barcodeText = postData.findValue("barCodeContent").asText();
//        QrCode qrCode = QrCode.encodeText(barcodeText, QrCode.Ecc.MEDIUM);
//        BufferedImage img = toImage(qrCode, 4, 10);
//
//        File outputfile = new File("/home/shubham/image.jpg");
//        try {
//            ImageIO.write(img, "jpg", outputfile);
//        } catch (IOException e) {
//            return ResponseEntity.accepted().body("error while generating barcode image");
//        }
//        return ResponseEntity.accepted().body("barcode generated successfully");
//    }
//
//    public  BufferedImage toImage(QrCode qr, int scale, int border) {
//        return toImage(qr, scale, border, 0xFFFFFF, 0x000000);
//    }
//
//    public BufferedImage toImage(QrCode qr, int scale, int border, int lightColor, int darkColor) {
//        Objects.requireNonNull(qr);
//        if (scale <= 0 || border < 0) {
//            throw new IllegalArgumentException("Value out of range");
//        }
//        if (border > Integer.MAX_VALUE / 2 || qr.size + border * 2L > Integer.MAX_VALUE / scale) {
//            throw new IllegalArgumentException("Scale or border too large");
//        }
//
//        BufferedImage result = new BufferedImage(
//                (qr.size + border * 2) * scale,
//                (qr.size + border * 2) * scale,
//                BufferedImage.TYPE_INT_RGB
//        );
//        for (int y = 0; y < result.getHeight(); y++) {
//            for (int x = 0; x < result.getWidth(); x++) {
//                boolean color = qr.getModule(x / scale - border, y / scale - border);
//                result.setRGB(x, y, color ? darkColor : lightColor);
//            }
//        }
//        return result;
//    }


    @GetMapping("/test-logging")
    public ResponseEntity<String> testLogging(){

        System.out.println("Hello!");
        logger.info("info");
        logger.debug("debug");
        logger.warn("warn");
        logger.error("error");


        return ResponseEntity.accepted().body("Works!");
    }

    @PostMapping("/validate")
    public ResponseEntity<String> validateStaticContent(HttpServletRequest request) {

        // 1. Print Request Method
        System.out.println("Request Method: " + request.getMethod());

        // 2. Print Request URI
        System.out.println("Request URI: " + request.getRequestURI());

        // 3. Print Request URL
        System.out.println("Request URL: " + request.getRequestURL().toString());

        // 4. Print Query String
        System.out.println("Query String: " + request.getQueryString());

        // 5. Print Protocol
        System.out.println("Protocol: " + request.getProtocol());

        // 6. Print Client IP Address
        System.out.println("Client IP Address: " + request.getRemoteAddr());

        // 7. Print Client Hostname
        System.out.println("Client Hostname: " + request.getRemoteHost());

        // 8. Print Server Name
        System.out.println("Server Name: " + request.getServerName());

        // 9. Print Server Port
        System.out.println("Server Port: " + request.getServerPort());

        // 10. Print Headers
        System.out.println("\nHeaders:");
        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            String headerValue = request.getHeader(headerName);
            System.out.println(headerName + ": " + headerValue);
        }

        // 11. Print Parameters
        System.out.println("\nParameters:");
        Enumeration<String> parameterNames = request.getParameterNames();
        while (parameterNames.hasMoreElements()) {
            String paramName = parameterNames.nextElement();
            String paramValue = request.getParameter(paramName);
            System.out.println(paramName + ": " + paramValue);
        }

        // 13. Print Content Type
        System.out.println("\nContent Type: " + request.getContentType());

        // 14. Print Content Length
        System.out.println("Content Length: " + request.getContentLength());

        // 15. Print Character Encoding
        System.out.println("Character Encoding: " + request.getCharacterEncoding());

        // 16. Print Scheme
        System.out.println("Scheme: " + request.getScheme());

        // 17. Print Local Address
        System.out.println("Local Address: " + request.getLocalAddr());

        // 18. Print Local Port
        System.out.println("Local Port: " + request.getLocalPort());

        // 19. Print Remote Port
        System.out.println("Remote Port: " + request.getRemotePort());

        // 20. Print Session ID (if any)
        if (request.getSession(false) != null) {
            System.out.println("Session ID: " + request.getSession().getId());
        } else {
            System.out.println("No session available.");
        }

       return ResponseEntity.accepted().body("validated");

    }

//    @Autowired
//    IKafkaMessageService kafkaMessageService;

    @GetMapping("/generateTemplate")
    public void createTemplate(){
        // Create a new workbook
        Workbook workbook = new XSSFWorkbook();

        // Create a sheet for the main content
        Sheet sheet = workbook.createSheet("Sheet1");

        // Create a hidden sheet for the crop-to-UUID mapping
        Sheet hiddenSheet = workbook.createSheet("Data");
        workbook.setSheetHidden(workbook.getSheetIndex(hiddenSheet), false);

        // List of crops and their UUIDs
        Map<String, String> cropVsCropUUID = new HashMap<>();
        cropVsCropUUID.put("Crop 1", "UUID 1");
        cropVsCropUUID.put("Crop 2", "UUID 2");
        cropVsCropUUID.put("Crop 3", "UUID 3");
        cropVsCropUUID.put("Crop 4", "UUID 4");

        // Populate the hidden sheet with the crop-to-UUID mapping
        int rowIndex = 1; // Start from row 1 (0 is the header row)
        Row headerRow = hiddenSheet.createRow(0);
        headerRow.createCell(0).setCellValue("Crop");
        headerRow.createCell(1).setCellValue("UUID");

        for (Map.Entry<String, String> entry : cropVsCropUUID.entrySet()) {
            Row row = hiddenSheet.createRow(rowIndex++);
            row.createCell(0).setCellValue(entry.getKey());
            row.createCell(1).setCellValue(entry.getValue());
        }

        // Define the range for the drop-down list (e.g., entire column A)
        int startRow = 1;
        int endRow = 100; // Change this to the desired number of rows

        // Create a row to hold the headers
        Row mainHeaderRow = sheet.createRow(0);
        mainHeaderRow.createCell(0).setCellValue("Crop");
        mainHeaderRow.createCell(1).setCellValue("CropUUID");

        // Create a drop-down list in column A
        DataValidationHelper validationHelper = new XSSFDataValidationHelper((XSSFSheet) sheet);
        DataValidationConstraint constraint = validationHelper.createExplicitListConstraint(cropVsCropUUID.keySet().toArray(new String[0]));
        CellRangeAddressList addressList = new CellRangeAddressList(startRow, endRow, 0, 0);
        DataValidation dataValidation = validationHelper.createValidation(constraint, addressList);
        sheet.addValidationData(dataValidation);

        // Auto-populate UUIDs based on the selected crop using VLOOKUP
        for (int i = startRow; i <= endRow; i++) {
            Row row = sheet.createRow(i);
            Cell cropCell = row.createCell(0);
            Cell uuidCell = row.createCell(1);

            // Use VLOOKUP to dynamically populate UUID based on the selected crop
            String formula = String.format(
                    "IF(A%d=\"\", \"\", VLOOKUP(A%d, Data!$A$2:$B$%d, 2, FALSE))",
                    i + 1, i + 1, cropVsCropUUID.size() + 1
            );
            uuidCell.setCellFormula(formula);

            // Lock the UUID cell to make it non-editable
            CellStyle lockedCellStyle = workbook.createCellStyle();
            lockedCellStyle.setLocked(true);
            uuidCell.setCellStyle(lockedCellStyle);
        }

        // Protect the sheet to make UUIDs non-editable
//        sheet.protectSheet("password");

        // Save the workbook to a file
        try (FileOutputStream fileOut = new FileOutputStream("/home/shubham/ExcelDropdownExample.xlsx")) {
            workbook.write(fileOut);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Closing the workbook
        try {
            workbook.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private Map<String, TreeMap<Long, String>> userVsCaptureTimeVsImagePath = null;
    Long milliesInHr = 3600000l;


    public Map<Long, String> getImagesForUser(Long previousSurveyTs, Long currSurveyTs, String userId){
        Map<Long, String> imageClickedTimeVsImagePath = new HashMap<>();

        // get all the images of user userVsDateVsImagePath
        if(userVsCaptureTimeVsImagePath == null) {

            userVsCaptureTimeVsImagePath = new HashMap<>();
            Path folderPath = Paths.get("/home/shubham/dcs/");

            try {
                // regex to extract time
                String pattern = "([A-Za-z]{3} [A-Za-z]{3} \\d{2} \\d{2}_\\d{2}_\\d{2} GMT\\+\\d{2}_\\d{2} \\d{4})";
                Pattern regex = Pattern.compile(pattern);

                // Using Files.list to list files in the directory (non-recursive)
                Files.list(folderPath)
                        .filter(Files::isRegularFile)  // Only regular files (ignore directories)
                        .forEach(filePath -> {

                            String imagePath = filePath.getFileName().toString();
                            System.out.println("File name: " + imagePath);

                            // Find the date part using regex
                            Matcher matcher = regex.matcher(imagePath);
                            if (matcher.find()) {
                                // Extract the timestamp string
                                String timestampStr = matcher.group(1);

                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy");

                                // Replace the underscores with colons for parsing
                                String timestampWithColon = timestampStr.replace("_", ":");

                                ZonedDateTime zonedDateTime = ZonedDateTime.parse(timestampWithColon, formatter);
                                System.out.println("Parsed Date: " + zonedDateTime + " millies ::  " + zonedDateTime.toInstant().toEpochMilli() );
                                // Parse the input string into a Date object
                                userVsCaptureTimeVsImagePath.computeIfAbsent(userId, k -> new TreeMap<>()).put(zonedDateTime.toInstant().toEpochMilli(), imagePath);
                            }
                        });
            } catch (IOException e) {
                System.out.println("Error reading the folder.");
                e.printStackTrace();
            }
        }


        // get all the images which are clicked b/w startTs and endTs

        for(Long captureTs : userVsCaptureTimeVsImagePath.get(userId).keySet()){

            if(captureTs >= previousSurveyTs && captureTs <= currSurveyTs){

                // get all the images which are clicked before 1 hr from currSurveyTs
                if(captureTs <= (currSurveyTs - milliesInHr)){
                    imageClickedTimeVsImagePath.put(captureTs, userVsCaptureTimeVsImagePath.get(userId).get(captureTs));
                }
            }
        }

        System.out.println("total  " + imageClickedTimeVsImagePath.size() + " image found for this servey");

        return imageClickedTimeVsImagePath;
    }


    // testReplicaFetch (GET /test-replica-fetch) removed from here - duplicated
    // com.vassarlabs.alerts.controller.AlertController#testReplicaFetch (wrims-core-platform's
    // alerts/service module, already bundled via the platform jar) with an identical signature,
    // causing an "Ambiguous mapping" startup failure. That controller already has the finished
    // version of this endpoint.

//    @GetMapping("/debug/kafkaPartitioner/{topicName}")
//    public void sendKafkaMessage(@RequestParam("topicName") String topicName){
//
//        try{
//            TestEvent message = new TestEvent();
//            message.setMessage("hello!");
//            kafkaMessageService.sendKafkaMessage(topicName, message);
//        } catch (KafkaSendMessageException e) {
//            e.printStackTrace();
//            throw new RuntimeException(e);
//        }
//
//    }
//
//
//    public class TestEvent implements IKafkaMessageObject {
//
//        String message;
//
//        @Override
//        public String getMessageUUID() {
//            return UUID.randomUUID().toString();
//        }
//
//        public void setMessage(String message){
//            this.message = message;
//        }
//
//        public String getMessage(){
//            return this.message;
//        }
//    }
}
