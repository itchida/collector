package ext.renault.phenix.datafix.sietranslationprocess;

import java.beans.PropertyVetoException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.ptc.arbortext.windchill.translation.TranslationLink;
import com.ptc.core.components.export.table.ExportException;
import com.ptc.core.components.export.table.ExportListWriter;
import com.ptc.core.components.export.table.ExportListWriterFactory;
import com.ptc.core.lwc.server.PersistableAdapter;
import com.ptc.core.meta.common.TypeIdentifier;
import com.ptc.windchill.uwgm.common.container.OrganizationHelper;

import ext.renault.phenix.interfaces.exportmd.repository.manager.Authorization;
import ext.renault.phenix.utilities.MediaUtils;
import wt.content.ApplicationData;
import wt.content.ContentItem;
import wt.content.ContentRoleType;
import wt.content.ContentServerHelper;
import wt.epm.EPMDocument;
import wt.fc.PersistenceHelper;
import wt.fc.PersistenceServerHelper;
import wt.fc.QueryResult;
import wt.fc.ReferenceFactory;
import wt.fc.WTReference;
import wt.fc.collections.WTHashSet;
import wt.fc.collections.WTSet;
import wt.identity.IdentityFactory;
import wt.inf.container.WTContained;
import wt.load.LoadServerHelper;
import wt.method.RemoteAccess;
import wt.method.RemoteMethodServer;
import wt.org.WTOrganization;
import wt.pdmlink.PDMLinkProduct;
import wt.pom.PersistenceException;
import wt.pom.Transaction;
import wt.query.ArrayExpression;
import wt.query.ClassAttribute;
import wt.query.OrderBy;
import wt.query.QuerySpec;
import wt.query.SearchCondition;
import wt.session.SessionHelper;
import wt.type.TypedUtilityServiceHelper;
import wt.util.WTAttributeNameIfc;
import wt.util.WTException;
import wt.util.WTProperties;
import wt.vc.VersionControlHelper;
import wt.vc.Versionable;
import wt.vc.config.LatestConfigSpec;


//usage:
//windchill ext.renault.phenix.datafix.sietranslationprocess.SieTranslationProcess -m DRY -o $WT_HOME/tmp/sieTranslationProcess/SieTranslationProcess_DRY.csv -type TM -org Renault -b $WT_HOME/tmp/sieTranslationProcess -u wcadmin -p wcadmin --log-level ALL
//windchill ext.renault.phenix.datafix.sietranslationprocess.SieTranslationProcess -m RECOVERY -i $WT_HOME/tmp/sieTranslationProcess/SieTranslationProcess_DRY.csv -b $WT_HOME/tmp/sieTranslationProcess -o $WT_HOME/tmp/sieTranslationProcess/SieTranslationProcess_RECOVERY.csv -u wcadmin -p wcadmin --log-level ALL
public class SieTranslationProcess implements RemoteAccess {

    /** The Constant CSV_OUPUT_TYPE. */
    private static final String CSV_OUPUT_TYPE = "csv";

    /** The Constant CSV_OUTPUT_HEADER. */
    private static final String CSV_OUTPUT_HEADER_FOR_SIE_UPDATE = "OBJECT_TYPE, OBJECT_ID, ORGANIZATION, LOCALE, SIE_NUMBER, SIE_TYPE, EXECUTED, RESULT, INFORMATION";

    /** The Constant CSV_SEPARATOR. */
    private static final String CSV_SEPARATOR = ",";

    /** The Constant CSV_ENCODING. */
    private static final String CSV_ENCODING = "UTF8";

    /** The Constant CSV_LIMIT_ARG. */
    private static final String CSV_LIMIT_ARG = "csv-limit";

    /** The Constant CLASS_NAME. */
    private static final String CLASS_NAME = SieTranslationProcess.class.getName();

    /** The Constant CLASS_SIMPLE_NAME. */
    private static final String CLASS_SIMPLE_NAME = SieTranslationProcess.class.getSimpleName();

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(CLASS_NAME);

    /** The Constant LOG_LEVELS. */
    private static final List<String> LOG_LEVELS = Arrays.asList("ALL", "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF");

    /** The Constant MODE_ARG. */
    private static final String MODE_ARG = "m";

    /** The Constant ORG_ARG. */
    private static final String ORG_ARG = "org";

    /** The Constant OBJECT_TYPE_ARG. */
    private static final String OBJECT_TYPE_ARG = "type";

    /** The Constant USER_ARG. */
    private static final String USER_ARG = "u";

    /** The Constant PASSW_ARG. */
    private static final String PASSW_ARG = "p";

    /** The Constant HELP_ARG. */
    private static final String HELP_ARG = "h";

    /** The Constant OUTPUT_ARG. */
    private static final String OUTPUT_ARG = "o";

    /** The Constant LONG_LOG_LEVEL_ARG. */
    private static final String LONG_LOG_LEVEL_ARG = "log-level";

    /** The Constant INPUT_ARG. */
    private static final String INPUT_ARG = "i";

    /** The Constant BACKUP_ARG */
    private static final String BACKUP_ARG = "b";

    /** The Constant NUMBER_RANGE */
    private static final String NUMBER_RANGE = "r";

    /** The Constant USAGE. */
    private static final String USAGE = "\nwindchill " + CLASS_NAME;

    /** The Constant WT_SERVER_CODEBASE. */
    private static final String WT_SERVER_CODEBASE = "wt.server.codebase";

    /** The Constant METHOD. */
    private static final String METHOD = "main";

    /** The Constant REFERENCE_FACTORY. */
    private static final ReferenceFactory REFERENCE_FACTORY = new ReferenceFactory();
    
    private static final String STATE_ATTRIBUTE_KEY = "state.state";
    
    private static final String MODE_RECOVERY = "recovery";
    private static final String MODE_DRY = "dry";

    private static final String PRINT_MESSAGE = "Data recovery for Sie translation Process";
    protected static final String LINE_PROGRESS = "Processing line (%s/%s)";
    protected static final String PRINT_UPDATE_SUCCESS = "Update SIE XML (translated = no) - Successful:";


    private static final String FEATURE_LOAD_DTD_GRAMMAR = "http://apache.org/xml/features/nonvalidating/load-dtd-grammar";
    private static final String FEATURE_LOAD_EXTERNAL_DTD = "http://apache.org/xml/features/nonvalidating/load-external-dtd";

    private static final Pattern regexNumberPatternGie = Pattern.compile("[A-Za-z].*-[0]*");

    private static final String PTC_DD_LANGUAGE = "PTC_DD_LANGUAGE";

    private static final String SIENAME_TAG = "siename";


    public static final String UPDATE_FAILED = "Update failed";
    public static final String UPDATE_SUCCESSFUL = "Update successful";
    public static final String UPDATE_SUCCESSFUL_WITH_WARNINGS = "Update successful with warnings";
    public static final String UPDATE_IGNORED = "Update ignored";

    public static final String PROCESSED = "Successfully processed";
    private static final String RATE_TAG = "rate";
    private static final String SEGMENT_CODE_TAG = "segmentcode";
    private static final String TEST_TYPE_TAG = "testtype";
    private static final String COMPUTER_SYS_REC_TAG = "computersystemrecipient";
    private static final String OPE_TYPE_TAG = "operationtype";
    private static final String TYPE_TAG = "type";
    
    private static Map<SieTranslationProcessType, List<String>> allowedAuthoringLangagesMap; 
    
    private static final String XPATH_TM_TITLE = "/labortime/title";
    private static final String XPATH_MR_TITLE = "/description/title|/procedure/title";
    private static final String XPATH_MD_DIAGTITLE = "/diagnostic/diagtitle|/diagnostic_adt/diagtitle";
    private static XPathExpression xpathTMTitle = null;
    private static XPathExpression xpathMRTitle = null;
    private static XPathExpression xpathMDDiagtitle = null;

    private static final String FR = "fr";
    private static final String EN = "en";
    private static final String RU = "ru";
    

    private static int epmToRecover = 0;
    private static int epmToIgnored = 0;
    private static int objSuccess = 0;
    private static int objIgnored = 0;
    private static int objFailed = 0;

    private static long orgId;
    private static Boolean run;
    
    static {
        // Authoring languages available for each type
        allowedAuthoringLangagesMap = new HashMap<>();
        allowedAuthoringLangagesMap.put(SieTranslationProcessType.MR_PROC,
                new ArrayList<>(Arrays.asList(FR, EN, RU)));
        allowedAuthoringLangagesMap.put(SieTranslationProcessType.MR_DESC,
                new ArrayList<>(Arrays.asList(FR, EN, RU)));
        allowedAuthoringLangagesMap.put(SieTranslationProcessType.TM,
                new ArrayList<>(Arrays.asList(FR, EN, RU)));
        allowedAuthoringLangagesMap.put(SieTranslationProcessType.DIAG_ADT,
                new ArrayList<>(Arrays.asList(EN)));
        allowedAuthoringLangagesMap.put(SieTranslationProcessType.DIAG,
                new ArrayList<>(Arrays.asList(EN, FR)));
        
        try {
            xpathTMTitle = XPathFactory.newInstance().newXPath().compile(XPATH_TM_TITLE);
            xpathMRTitle = XPathFactory.newInstance().newXPath().compile(XPATH_MR_TITLE);
            xpathMDDiagtitle = XPathFactory.newInstance().newXPath().compile(XPATH_MD_DIAGTITLE);
        } catch (XPathExpressionException e) {
            LOGGER.error(e);
        }
        
    }

    /**
     * The Enum ExecutionMode.
     */
    private enum ExecutionMode {

        /** The dry. */
        DRY("the DRY mode."),
        /** The recovery. */
        RECOVERY("the RECOVERY mode.");

        /** The description. */
        private final String description;

        /**
         * Instantiates a new execution mode.
         *
         * @param description
         *            the description
         */
        ExecutionMode(String description) {
            this.description = description;
        }

        /**
         * Gets the description.
         *
         * @return the description
         */
        public String getDescription() {
            return description;
        }

        /**
         * Resolve.
         *
         * @param value
         *            the value
         * @return the execution mode
         */
        public static ExecutionMode resolve(String value) {
            for (ExecutionMode executionMode : ExecutionMode.values()) {
                if (executionMode.name().equalsIgnoreCase(value)) {
                    return executionMode;
                }
            }
            return null;
        }
    }

    /**
     * The main method, of course.
     *
     * @param args
     *            command line arguments
     * @throws Exception
     *             the exception
     */
    public static void main(String[] args) throws Exception {

        if (RemoteMethodServer.ServerFlag) {
            SessionHelper.manager.getPrincipal();
            CommandLine commandLine = getCommandLineFromArgs(args);
            String logdir = WTProperties.getServerProperties().getProperty("wt.logs.dir");
            PatternLayout layout = new PatternLayout("%d{ISO8601} %-5p [%t] - %m%n");
            String dateString = new SimpleDateFormat("yyMMdd_hh_mm").format(new Date());
            String path = new StringBuilder(logdir).append(File.separator).append("datafix").append(File.separator).append(CLASS_SIMPLE_NAME).append(File.separator).append(CLASS_SIMPLE_NAME).append("_").append(dateString).append(".log").toString();
            FileAppender appender = new FileAppender(layout, path, true);
            appender.setImmediateFlush(true);
            appender.activateOptions();
            appender.setName(CLASS_SIMPLE_NAME);
            LOGGER.addAppender(appender);
            LOGGER.setLevel(Level.toLevel(getLogLevelOption(commandLine), Level.ALL));
            LOGGER.info("############################################################");
            LOGGER.info("Starting SIE translation Process Dry/Recovery Tool.");
            LOGGER.info("Logged in to a method server. Executing command");

            try {
                runMode(commandLine);
            } catch (Exception e) {
                LOGGER.debug(e, e);
                LOGGER.error("Problem during runMode.", e);
                LoadServerHelper.printMessage(PRINT_MESSAGE + " ended with errors.");
            } finally {
                LOGGER.info("Ending SIE translation Process Dry/Recovery Tool.");
                LOGGER.info("############################################################");
                LOGGER.removeAppender(appender);
            }

        } else {
            //validate the commande line
            CommandLine commandLine = null;
            try {
                commandLine = getCommandLineFromArgs(args);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                HelpFormatter formatter = new HelpFormatter();
                formatter.setWidth(120);
                formatter.printHelp(USAGE, getOptions());
                System.exit(1);
            }
            //remote call of the main method in methodserver
            if (commandLine != null) {
                final RemoteMethodServer server = acquireServer();
                Authorization authorization = loginCommand(commandLine);
                String username = authorization.getUser();
                String password = authorization.getPassw();
                authorization = null;

                if (!username.isEmpty() && !password.isEmpty()) {
                    server.setUserName(username);
                    server.setPassword(password);
                    server.invoke(METHOD, CLASS_NAME, null, new Class[] { String[].class }, new Object[] { args });
                }
            }
        }
    }

    /**
     * Run mode.
     *
     * @param commandLine
     *            the command line
     * @throws ParseException 
     * @throws WTException 
     * @throws Exception
     *             the exception
     */
    private static void runMode(CommandLine commandLine) throws WTException, ParseException {

        ExecutionMode executionMode = null;
        if (commandLine.hasOption(MODE_ARG)) {
            String modeOption = commandLine.getOptionValue(MODE_ARG);
            executionMode = ExecutionMode.resolve(modeOption);
        }

        if (executionMode == null) {
            throw new ParseException(MODE_ARG + " not declared");
        }

        switch (executionMode) {
            case DRY:
                run=false;
                runDry(commandLine);
                break;
            case RECOVERY:
                run=true;
                runRecovery(commandLine);
                break;
            default:
                throw new IllegalArgumentException("Mode argument is not valid");
        }
    }

    /**
     * Run dry.
     *
     * @param commandLine
     *            the command line
     * @throws WTException 
     * @throws ParseException 
     * @throws Exception
     *             the exception
     */
    private static void runDry(CommandLine commandLine) throws WTException, ParseException {

        LOGGER.info("Starting " + ExecutionMode.DRY.getDescription());
        Date startDate = new Date();

        String outputOption = getOutputOption(commandLine);
        String numberRange = getNumberRangeOption(commandLine);
        SieTranslationProcessType type = getTypeOption(commandLine);
        long csvRowsLimit = getCsvLimitOption(commandLine);
        String backupDir = getBackupOption(commandLine);
        backupDir = addDateToBackupDir(backupDir);

        epmToRecover = 0;
        epmToIgnored = 0;
       
        runDryForSie(commandLine, outputOption, numberRange, type, csvRowsLimit, backupDir);
       

        Date endDate = new Date();
        Map<TimeUnit, Long> elapsedTimeData = computeDiff(startDate, endDate);
        String elapsedTimeMessage = "Elapsed time: " + elapsedTimeData;

        LoadServerHelper.printMessage(PRINT_MESSAGE + ", EPMDocument needing recovery: " + epmToRecover + ", EPMDocument ignored: " + epmToIgnored);
        LOGGER.info(PRINT_MESSAGE + ", EPMDocument needing recovery: " + epmToRecover + ", EPMDocument ignored: " + epmToIgnored);
        LoadServerHelper.printMessage(elapsedTimeMessage);

        LOGGER.info(elapsedTimeMessage);
        LOGGER.info("Ending " + ExecutionMode.DRY.getDescription());
    }

    /**
     * Run dry. 
     * Retrieves Translated SIE (source + translated) By Type Option.
     * 
     * @param commandLine
     * @param outputOption
     * @param numberRange
     * @param type
     * @param csvRowsLimit
     * @param backupDir 
     * @throws WTException
     */
    private static void runDryForSie(CommandLine commandLine, String outputOption, String numberRange, SieTranslationProcessType type, long csvRowsLimit, String backupDir) throws WTException {
        ExportListWriter outputWriter = null;
        File outputFile = new File(outputOption);
        outputFile.getParentFile().mkdirs();
        List<String> allowedAuthoringLanguages = allowedAuthoringLangagesMap.get(type);
        String orgName = commandLine.getOptionValue(ORG_ARG);
        orgId = getOrgId(orgName);

        try (FileOutputStream os = new FileOutputStream(outputFile);
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(os, CSV_ENCODING), ExportListWriter.BUFFER_SIZE);) {
            outputWriter = ExportListWriterFactory.getExportWriter(CSV_OUPUT_TYPE);
            outputWriter.setFileName(outputFile.getName());
            outputWriter.setOut(out);
            outputWriter.writeColumnHeaders(CSV_OUTPUT_HEADER_FOR_SIE_UPDATE, CSV_SEPARATOR);

            Long currentFileLines = new Long(0);

            // Gets all Source SIE of type 'type'
            List<BigDecimal> siesSet = getAllSourceSie(type, numberRange);

            
            // Get All Eligible SIE for recovery
            WTSet sieSet = new WTHashSet();
            for (BigDecimal sieRef : siesSet) {
                EPMDocument sie = getSIE(sieRef);
                LOGGER.debug("Start processing SIE [" + sie.getNumber() + ", " + sie.getDisplayIdentifier() + "]");
                if(sie != null && !sieSet.contains(sie) && sieNeedsRecovery(sie, type, backupDir)) {
                    LOGGER.debug("SIE " + sie.getNumber() + " eligible for recovery");
                    sieSet.add(sie);
                    // Get the SIE translations
                    Set<EPMDocument> translationSet = getTranslations(sie);
                    for(EPMDocument translatedSIE : translationSet) {
                        String authLanguage = getSieLanguage(translatedSIE);
                        authLanguage = StringUtils.lowerCase(authLanguage);
                        // check if the translation SIE needs a recovery
                        if(allowedAuthoringLanguages.contains(authLanguage) && sieNeedsRecovery(translatedSIE, type, backupDir)) {
                            LOGGER.debug(String.format("Translation %s of SIE source %s with auth language %s eligible for recovery", translatedSIE.getNumber(), sie.getNumber(), authLanguage));
                            sieSet.add(translatedSIE);
                        }
                    }
                    
                }
            }
            
            Iterator iterator = sieSet.persistableIterator();
            
            while(iterator.hasNext()) {
                EPMDocument sie = (EPMDocument) iterator.next();
                SIEData sieData = new SIEData(getSieLanguage(sie), REFERENCE_FACTORY.getReference(sie), sie.getName(), sie.getNumber(), type.name());
                SieTranslationProcessRow csvRow = new SieTranslationProcessRow(sieData);
                epmToRecover++;
                currentFileLines++;
                csvRow.setProcessed();

                List<Object> writerValues = writeDryRowData(csvRow.getLineRow(), outputWriter, outputFile, currentFileLines, outputOption, csvRowsLimit, false);
                currentFileLines = (Long) writerValues.get(0);
                outputWriter = (ExportListWriter) writerValues.get(1);
                outputOption = (String) writerValues.get(2);
                outputFile = (File) writerValues.get(3);
            }


            LOGGER.info("Writing output file: " + outputFile.getAbsolutePath());
            outputWriter.close();

        } catch (Exception e) {
            throw new WTException(e, "An error occured while trying to create output report file.");
        }
    }
    

    /**
     * Check in xml content if the translate attribute is set to "yes"
     * @param sie
     * @param type
     * @param backupDir
     * @return
     * @throws Exception 
     */
    private static boolean sieNeedsRecovery(EPMDocument sie, SieTranslationProcessType type, String backupDir) throws Exception {
        boolean needsRecovery = false;
        Document sieContent = parseEPMDocumentToDOM(sie);
        if(sieContent != null) {
            try {
                if(StringUtils.equals(type.getName(), SieTranslationProcessType.MR_DESC.name())      || 
                        StringUtils.equals(type.getName(), SieTranslationProcessType.MR_PROC.name())){
                    needsRecovery = hasTranslateEqualTrue(sieContent, SIENAME_TAG) ||
                            isSIEtitleTranslatedEqualTrue(xpathMRTitle, sieContent);
                }  else if(StringUtils.equals(type.getName(), SieTranslationProcessType.TM.name())) {
                    needsRecovery =  isSIEtitleTranslatedEqualTrue(xpathTMTitle, sieContent)            ||
                            hasTranslateEqualTrue(sieContent, SIENAME_TAG)          ||
                            hasTranslateEqualTrue(sieContent, RATE_TAG)             ||
                            hasTranslateEqualTrue(sieContent, SEGMENT_CODE_TAG)     ||
                            hasTranslateEqualTrue(sieContent, TEST_TYPE_TAG)        ||
                            hasTranslateEqualTrue(sieContent, OPE_TYPE_TAG)         ||
                            hasTranslateEqualTrue(sieContent, COMPUTER_SYS_REC_TAG) ||
                            hasTranslateEqualTrue(sieContent, TYPE_TAG);
                } else if(StringUtils.equals(type.getName(), SieTranslationProcessType.DIAG.name()) || 
                        StringUtils.equals(type.getName(), SieTranslationProcessType.DIAG_ADT.name())) {
                    needsRecovery = isSIEtitleTranslatedEqualTrue(xpathMDDiagtitle, sieContent);
                }
            } catch(WTException e) {
                LOGGER.error("Missing content for SIE : " + sie.getNumber(), e);
                return false;
            }
            if(needsRecovery) {
                File f = parseDocumentToFile(sieContent);
                archiveContent(sie, f, backupDir);
                modifyEPMDocument(sieContent, sie, type.name(), false, backupDir);
            }
        }
        return needsRecovery;
    }

    /**
     * Specific method to retrieve only the principal title
     * @param sieContent
     * @return
     * @throws WTException
     * @throws XPathExpressionException
     */
    private static boolean isSIEtitleTranslatedEqualTrue(XPathExpression xpath, Document sieContent) throws WTException, XPathExpressionException {
        
        NodeList diagTitleNodeList = (NodeList) xpath.evaluate(sieContent, XPathConstants.NODESET);
        if (diagTitleNodeList.getLength() == 0){
            LOGGER.error("No <title> tag found in XML.");
            return false;
        } else if (diagTitleNodeList.getLength()>1){
            LOGGER.error("More than one <title> tag value found in XML");
            return false;
        } else {
            Element sieElem = (Element) diagTitleNodeList.item(0);
            String translateValue = sieElem.getAttribute("translate");
            return "yes".equals(translateValue);
        }
    }

    private static boolean hasTranslateEqualTrue(Document sieContent, String tagName) throws WTException {
        NodeList sieNodeList = sieContent.getElementsByTagName(tagName);
        if (sieNodeList.getLength() == 0){
            LOGGER.error("No " + tagName + " tag found in XML.");
            return false;
        } else if (sieNodeList.getLength()>1){
            LOGGER.error("More than one " + tagName + " tag value found in XML");
            return false;
        } else {
            Element sieElem = (Element) sieNodeList.item(0);
            String translateValue = sieElem.getAttribute("translate");
            return "yes".equals(translateValue);
        }
        
    }

    /**
     * Run recovery.
     *
     * @param commandLine
     *            the command line
     * @throws WTException 
     * @throws ParseException
     *             the exception
     */
    private static void runRecovery(CommandLine commandLine) throws ParseException, WTException {
         LOGGER.info("Starting " + ExecutionMode.RECOVERY.getDescription());
         String inputOption = getInputOption(commandLine);
         String outputOption = getOutputOption(commandLine);
         String backupDir = getBackupOption(commandLine);
         backupDir = addDateToBackupDir(backupDir);
         
         File inputFile = new File(inputOption);
         LOGGER.info("Reading " + inputFile.getAbsolutePath());
         
         objSuccess = 0;
         objIgnored = 0;
         objFailed = 0;
         
         Date startDate = new Date();

         ExportListWriter outputWriter = null;

         CSVFormat csvFormat = CSVFormat.newFormat(',').withSkipHeaderRecord(true).withQuote('"').withHeader().withIgnoreEmptyLines(true).withRecordSeparator("\r\n");

         File outputFile = new File(outputOption);
         do {
             try (CSVParser parser = CSVParser.parse(inputFile, StandardCharsets.UTF_8, csvFormat);
                     FileOutputStream os = new FileOutputStream(outputFile);
                     BufferedWriter out = new BufferedWriter(new OutputStreamWriter(os, CSV_ENCODING), ExportListWriter.BUFFER_SIZE);) {

                 outputWriter = ExportListWriterFactory.getExportWriter(CSV_OUPUT_TYPE);
                 outputWriter.setFileName(outputFile.getName());
                 outputWriter.setOut(out);
                 outputWriter.writeColumnHeaders(CSV_OUTPUT_HEADER_FOR_SIE_UPDATE, CSV_SEPARATOR);

                 List<CSVRecord> records = parser.getRecords();
                 String messageFormat = "Found %d records to update in CSV file (%s).";
                 LOGGER.info(String.format(messageFormat, records.size(), inputFile));

                 runRecoveryForSie(records, outputWriter, backupDir);

             } catch (IOException ioe) {
                 throw new WTException(ioe);
             } catch (ExportException ee) {
                 throw new WTException(ee, "An error occured while trying to create output report file.");
             }

             outputOption = incrementFile(outputOption);
             outputFile = new File(outputOption);
         } while (outputFile != null && outputFile.exists());

         Date endDate = new Date();
         Map<TimeUnit, Long> elapsedTimeData = computeDiff(startDate, endDate);
         String elapsedTimeMessage = "Elapsed time: " + elapsedTimeData;

         String result = PRINT_UPDATE_SUCCESS + objSuccess + ", Failed: " + objFailed + ", Ignored: " + objIgnored;
         LoadServerHelper.printMessage(result);
         LOGGER.info(result);
         LoadServerHelper.printMessage(elapsedTimeMessage);

         LOGGER.info("Ending " + ExecutionMode.RECOVERY.getDescription());
    }


    private static EPMDocument getSIE(BigDecimal sieRef) throws WTException {
        EPMDocument sie = null;
        try {
            WTReference sieref = REFERENCE_FACTORY.getReference(EPMDocument.class.getName() + ":" + String.valueOf(sieRef));
            sie = (EPMDocument) sieref.getObject();
            sie = (EPMDocument) getLatestVersionIteration(sie);
        } catch(Exception e){
            String errorMsg = "Impossible to retrieve reference : " + EPMDocument.class.getName() + ":" + String.valueOf(sieRef);
            LOGGER.error(errorMsg,e);
            throw new WTException(errorMsg);
        }
        return sie;
    }
    
    /**
     * Updates the SIE XML (translate = no)
     * @param records
     * @param outputWriter
     */
    private static void runRecoveryForSie(List<CSVRecord> records, ExportListWriter outputWriter, String backupDir) {
        Boolean executed = null;
        String errorMessage = null;
        int idx = 0;
        for (CSVRecord record : records) {
            LOGGER.info(String.format(LINE_PROGRESS, ++idx, records.size()));

            SieTranslationProcessRow lineRow = new SieTranslationProcessRow(record);
            
            // get object by its id
            String objectID = record.get(SieTranslationProcessRow.OBJECT_ID);
            String objectType = record.get(SieTranslationProcessRow.OBJECT_TYPE);
            String objectNumber = record.get(SieTranslationProcessRow.SIE_NUMBER);
            String sieType = record.get(SieTranslationProcessRow.SIE_TYPE);
            executed = Boolean.valueOf(record.get(SieTranslationProcessRow.EXECUTED));
    
            String objDisplay = null;
            try {
                String refString = objectType + ":" + objectID;
                
                if (executed) {
                    objIgnored++;
                    LOGGER.info("No update required for document (according to input file data): " + refString);
                    lineRow.setInfo("Already processed in previous run");
                } else {
                    WTReference wtref = REFERENCE_FACTORY.getReference(refString);
                    if (wtref == null) {
                        throw new WTException("Reference does not exist : " + refString);
                    }
    
                    if (wtref.getObject() instanceof EPMDocument) {
                        EPMDocument epmDocument = (EPMDocument) wtref.getObject();
                        
                        objDisplay = objectNumber + "," + IdentityFactory.getDisplayIdentifier(epmDocument).toString();
                        LOGGER.info("Starting update of " + objDisplay);
                        
                        handleSIE(epmDocument, sieType, backupDir, true);
                        
                    }
                    
                    objSuccess++;
                    lineRow.setSuccess();
                    LOGGER.info(refString + " has been renamed.");
                }
    
                LOGGER.info("Ending update of " + objDisplay);
            } catch (Exception e) {
                objFailed++;
                try {
                    if (e.getLocalizedMessage() != null) {
                        errorMessage = e.getLocalizedMessage().replaceAll("\r?\n", " ");
                    } else if (e.getMessage() != null) {
                        errorMessage = e.getMessage().replaceAll("\r?\n", " ");
                    } else {
                        errorMessage = "Null error Message. Exception is : " + e.toString().replaceAll("\r?\n", " ");
                    }
                    lineRow.setError(errorMessage);
                    if (objDisplay != null) {
                        LOGGER.error("An error occurs during the renaming of " + objDisplay, e);
                    } else {
                        LOGGER.error("An error occurs during the renaming of " + objectID, e);
                    }
                } catch (Exception ee) {
                    LOGGER.fatal("An error occured while storing result of document in CSV file", ee);
                }
    
            } finally {
                try {
                    outputWriter.writeRowData(lineRow.getLineRow());
                } catch (Exception ee) {
                    LOGGER.fatal("An error occured while storing result of document in CSV file", ee);
                }
    
            }
        }
    }

    /**
     * Get all the source SIE by type option 
     * Filter by states if mentionned in property file
     * Filter by number range if mentionned in options
     * @param type
     * @param numberRange
     * @return
     * @throws WTException
     */
    private static List<BigDecimal> getAllSourceSie(SieTranslationProcessType type, String numberRange) throws WTException {
        List<BigDecimal> sieMap = new ArrayList<>();
        Properties datafixProperties = SieTranslationProcessProperties.getProperties();
        String ignoredStates = datafixProperties.getProperty(SieTranslationProcessProperties.STATES_TO_IGNORE);
        try {
            TypeIdentifier ti = type.getSIESoftType();
            QuerySpec qs = new QuerySpec();
            int idxEpmDoc = qs.appendClassList(EPMDocument.class, false);
            SearchCondition latestSc = new SearchCondition(EPMDocument.class, WTAttributeNameIfc.LATEST_ITERATION, SearchCondition.IS_TRUE);
            SearchCondition orgSc = new SearchCondition(EPMDocument.class, EPMDocument.ORGANIZATION_REFERENCE + "." + WTAttributeNameIfc.REF_OBJECT_ID, SearchCondition.EQUAL, orgId);
            SearchCondition typeSc = TypedUtilityServiceHelper.service.getSearchCondition(ti, true);
            SearchCondition stateSc = null;
            if(!StringUtils.isEmpty(ignoredStates)) {
                ArrayExpression arrayExpression = new ArrayExpression(ignoredStates.split(","));
                stateSc = new SearchCondition(new ClassAttribute(EPMDocument.class, STATE_ATTRIBUTE_KEY), SearchCondition.NOT_IN, arrayExpression);
            }
            //Search on parent container = PDMLinkProduct
            SearchCondition sourceSc = new SearchCondition(EPMDocument.class, WTContained.CONTAINER_REFERENCE + "." + WTAttributeNameIfc.REF_CLASSNAME, SearchCondition.EQUAL, PDMLinkProduct.class.getName());

            
            qs.appendSelect(new ClassAttribute(EPMDocument.class, "thePersistInfo.theObjectIdentifier.id"), new int[] { idxEpmDoc }, false);
            qs.appendSelect(new ClassAttribute(EPMDocument.class, EPMDocument.NUMBER), new int[] { idxEpmDoc }, false);

            qs.appendWhere(typeSc, new int[] { idxEpmDoc });
            qs.appendAnd();
            qs.appendWhere(latestSc, new int[] { idxEpmDoc });
            qs.appendAnd();
            qs.appendWhere(orgSc, new int[] { idxEpmDoc });
            qs.appendAnd();
            qs.appendWhere(sourceSc, new int[] { idxEpmDoc });
            if(stateSc != null) {
                qs.appendAnd();
                qs.appendWhere(stateSc, new int[] { idxEpmDoc });
            }

            //sort by SIE number
            OrderBy orderByNumber = new OrderBy(new ClassAttribute(EPMDocument.class, EPMDocument.NUMBER), false);
            qs.appendOrderBy(orderByNumber, new int[] { idxEpmDoc });

            LOGGER.debug("getAllSourceSie type:" + ti + " qs:" + qs);
            LatestConfigSpec lcs = new LatestConfigSpec();
            qs = lcs.appendSearchCriteria(qs);
            QueryResult qr = PersistenceServerHelper.manager.query(qs);

            if (qr != null){
                LOGGER.info("Found " + qr.size() + " " + type.getSIESoftType().getTypename());
                sieMap = new ArrayList<>();

                while(qr.hasMoreElements()){
                    boolean isNumberInRange = (numberRange == null);
                    Object[] resultArray = (Object[]) qr.nextElement();
                    BigDecimal currentIda2a2 = (BigDecimal) resultArray[0];
                    String number = (String) resultArray[1];
                    if (!isNumberInRange) {
                        isNumberInRange = checkIfNumberInRange(number, numberRange);
                    }
                    
                    if (isNumberInRange) {
                        sieMap.add(currentIda2a2);
                    }
                }

                if (numberRange != null) {
                    LOGGER.info("Found " + sieMap.size() + " " + type.getSIESoftType().getTypename() + " in range " + numberRange);
                }
            }

        } catch (WTException wte) {
            throw new WTException(wte, "An error occured while retrieving SIEs list.");
        } 
        return sieMap;
    }

    private static boolean checkIfNumberInRange(String number, String numberRange) {
        try {
            Matcher matcher = regexNumberPatternGie.matcher(number);
            if (matcher.find()) {
                int value = Integer.parseInt(number.substring(matcher.end(), number.length()));
                int min = Integer.parseInt(numberRange.substring(0, numberRange.indexOf("-")));
                int max = Integer.parseInt(numberRange.substring(numberRange.indexOf("-") + 1, numberRange.length()));
                if (min <= value && value <= max) {
                    return true;
                }
            }

        }catch (NumberFormatException numberException) {
            LOGGER.error("Number format cannot be casted to an Integer! => " + number);
            return true;
        }

        return false;
    }

    /**
     * Gets locale from the SIE
     * @param document
     * @return locale of the SIE
     * @throws WTException
     */
    private static String getSieLanguage(EPMDocument document) throws WTException {
        String language = null;

        PersistableAdapter pa = new PersistableAdapter(document, null, null, null);
        pa.load(PTC_DD_LANGUAGE);
        language = (String) pa.get(PTC_DD_LANGUAGE);

        return language;
    }

    private static synchronized File parseDocumentToFile(Document newContent) throws WTException {
        try {
            // Standard XML ouputter
            TransformerFactory factory = TransformerFactory.newInstance();
            Transformer t = factory.newTransformer();

            Map<String, String> outputProperties = getOutputProperties();
            if (outputProperties != null) {
                for (String key : outputProperties.keySet()) {
                    t.setOutputProperty(key, outputProperties.get(key));
                }
            }

            // Create new temp file
            File file = File.createTempFile("EPMDocument", ".xml");
            if (!file.exists()) {
                file.createNewFile();
            }

            // Write DOM inside new file
            t.transform(new DOMSource(newContent), new StreamResult(file));

            return file;

        } catch (IOException | TransformerException e) {
            throw new WTException(e);
        }
    }

    /**
     * Row format : "OBJECT_TYPE","OBJECT_ID","ORGANIZATION","LOCALE","SIE_NUMBER","SIE_TYPE","EXECUTED","RESULT","INFORMATION"
     * @param csvRows
     * @param outputWriter
     * @param outputFile
     * @param currenFileLines
     * @param outputOption
     * @param csvRowsLimit
     * @param isDicoEntryUpdate
     * @return
     * @throws WTException
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     */
    private static List<Object> writeDryRowData(List<String> csvRows, ExportListWriter outputWriter, File outputFile, Long currenFileLines, String outputOption, long csvRowsLimit, boolean isDicoEntryUpdate) throws WTException, FileNotFoundException, UnsupportedEncodingException {
        List<Object> returnValue = null;

        //if CSV limit is reached then close the current writer and create an incremented one
        if (csvRowsLimit != -1 && currenFileLines.longValue() >= csvRowsLimit) {
            currenFileLines = new Long(0);
            outputWriter.close();
            outputWriter = null;

            //create incremented output File
            outputOption = incrementFile(outputOption);
            outputFile = new File(outputOption);
            FileOutputStream os = new FileOutputStream(outputFile);
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(os, CSV_ENCODING), ExportListWriter.BUFFER_SIZE);

            outputWriter = ExportListWriterFactory.getExportWriter(CSV_OUPUT_TYPE);
            outputWriter.setFileName(outputFile.getName());
            outputWriter.setOut(out);
            outputWriter.writeColumnHeaders(CSV_OUTPUT_HEADER_FOR_SIE_UPDATE, CSV_SEPARATOR);
        }

        LOGGER.info("Log line in " + outputFile + " : " + csvRows);
        outputWriter.writeRowData(csvRows);

        returnValue = new ArrayList<>();
        returnValue.add(currenFileLines);
        returnValue.add(outputWriter);
        returnValue.add(outputOption);
        returnValue.add(outputFile);

        return returnValue;
    }

    /**
     * Increment file name
     *
     * @param filePath
     * @return
     * @throws WTException
     */
    public static String incrementFile(String filePath) throws WTException {
        String returnPath = null;
        if (filePath == null) {
            throw new WTException("File path is not defined");
        }

        //retrieve extension index
        int lastIndexOfDot = filePath.lastIndexOf(".");
        if (lastIndexOfDot == -1) {
            throw new WTException("File path has no extension");
        }

        String baseName = filePath.substring(0, lastIndexOfDot);
        LOGGER.debug("File baseName = " + baseName);
        String extension = filePath.substring(lastIndexOfDot + 1, filePath.length());
        LOGGER.debug("extension = " + extension);
        if (extension.contains("/")) {
            throw new WTException("File path has no extension");
        }

        //retrieve delimiter index
        int lastIndexOfDelimiter = baseName.lastIndexOf("_part");
        if (lastIndexOfDelimiter == -1) {
            //no delimiter, previous path was the basePath
            baseName = baseName + "_part2";
            returnPath = baseName + "." + extension;
        } else {
            String indexString = baseName.substring(lastIndexOfDelimiter + 5, baseName.length());
            int index = Integer.valueOf(indexString).intValue();
            index++;
            baseName = baseName.substring(0, lastIndexOfDelimiter);
            baseName = baseName + "_part" + index;
            returnPath = baseName + "." + extension;
        }

        return returnPath;
    }

    private static void handleSIE(EPMDocument sie, String sieType, String backupDir, boolean fixfile) throws Exception{
        Document sieDOM = parseEPMDocumentToDOM(sie);
        if(sieDOM != null) {
            File f = parseDocumentToFile(sieDOM);
            archiveContent(sie, f, backupDir);
            modifyEPMDocument(sieDOM, sie, sieType, fixfile, backupDir);
        } else {
            throw new WTException("Error while parsing SIE : " + sie.getNumber());
        }
    }

    /**
     * Get XML content from an SIE
     * @param sie
     * @param directory
     * @return
     * @throws WTException
     */
    private static Document parseEPMDocumentToDOM(EPMDocument sie) {
        LOGGER.trace("parseEPMDocumentToDOM - start");
        Document output = null;
        try {
            // Get primary content
            ContentItem item = MediaUtils.getPrimaryContent(sie);
            if (item == null) {
                // No primary content
                LOGGER.error("Error while retreiving primary content for SIE : " + sie.getNumber());
            }
            // Get Byte data
            File f = MediaUtils.getContentAsFile(item);
            // Standard XML DOM parser
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setCoalescing(false);
            factory.setValidating(false);
            factory.setIgnoringElementContentWhitespace(true);
            factory.setXIncludeAware(false);
            // SVG files should use name spaces
            factory.setNamespaceAware(true);
            // Adding specific feature to avoid error on RNO environment
            factory.setFeature(FEATURE_LOAD_DTD_GRAMMAR, false);
            factory.setFeature(FEATURE_LOAD_EXTERNAL_DTD, false);

            output = factory.newDocumentBuilder().parse(f);
            f.delete();
           
        } catch (SAXException | IOException | ParserConfigurationException | WTException e) {
            LOGGER.error("Error while parsing SIE : " + sie.getNumber(), e);
        }
        LOGGER.trace("parseEPMDocumentToDOM - end");
        return output;
    }


    private static void archiveContent(EPMDocument sie, File f, String directory) throws IOException {
        File archive = new File(directory + "/" + sie.getNumber() + "_"
                + sie.getVersionDisplayIdentifier() + ".xml");
        if (archive.exists()) {
            archive.delete();
        }
        FileUtils.moveFile(f, new File(directory + "/" + sie.getNumber() + "_"
                + sie.getVersionDisplayIdentifier() + ".xml"));
        f.delete();
        
    }

    /**
     * Update the xml content of the sie
     * Update the content of siename and title tags with replaceWith value
     * Set translate = false on Templates
     *   for title for SIE MR/TM
     *   for diagtitle for DIAG/DIAG ADT
     *   for siename  on SIE MR/TM
     *   on rate,segmentcode,testtype,operationtype,computersystemrecipient,type for sie TM
     * @param sieDOM
     * @param sie
     * @param replaceWith
     * @param upload
     * @param backupDir
     * @throws Exception
     */
    private static void modifyEPMDocument(Document sieDOM, EPMDocument sie, String sieType, boolean upload, String backupDir) throws Exception{
        LOGGER.trace("modifyEPMDocument - start");
        
        if(StringUtils.equals(sieType, SieTranslationProcessType.MR_DESC.name()) || 
                StringUtils.equals(sieType, SieTranslationProcessType.MR_PROC.name())){
            updateTranslateAttribute(sieDOM, SIENAME_TAG);
            updateTitleTranslateAttribute(xpathMRTitle, sieDOM);
            
        } else if(StringUtils.equals(sieType, SieTranslationProcessType.TM.name())) {
            updateTranslateAttribute(sieDOM, SIENAME_TAG);
            updateTranslateAttribute(sieDOM, RATE_TAG);
            updateTranslateAttribute(sieDOM, SEGMENT_CODE_TAG);
            updateTranslateAttribute(sieDOM, TEST_TYPE_TAG);
            updateTranslateAttribute(sieDOM, OPE_TYPE_TAG);
            updateTranslateAttribute(sieDOM, COMPUTER_SYS_REC_TAG);
            updateTranslateAttribute(sieDOM, TYPE_TAG);
            updateTitleTranslateAttribute(xpathTMTitle, sieDOM);
            
        } else if(StringUtils.equals(sieType, SieTranslationProcessType.DIAG.name()) || 
                StringUtils.equals(sieType, SieTranslationProcessType.DIAG_ADT.name())) {
            updateTitleTranslateAttribute(xpathMDDiagtitle, sieDOM);
        }
        
        File modFile = parseDocumentToFile(sieDOM);
        uploadOrSave(sie, modFile, upload, backupDir);

        LOGGER.trace("modifyEPMDocument - end");
    }

    private static void updateTitleTranslateAttribute(XPathExpression xpath, Document sieDOM) throws XPathExpressionException {
        NodeList sieNodeList = (NodeList) xpath.evaluate(sieDOM, XPathConstants.NODESET);
        if (sieNodeList.getLength() == 0){
            LOGGER.error("No <title> tag found in XML.");
        } else if (sieNodeList.getLength()>1){
            LOGGER.error("More than one <title> tag value found in XML");
        } else {
            Element sieElem = (Element) sieNodeList.item(0);
            sieElem.setAttribute("translate", "no");
        }
        
    }

    private static void updateTranslateAttribute(Document sieDOM, String tagName) {
        NodeList sieNodeList = sieDOM.getElementsByTagName(tagName);
        if (sieNodeList.getLength() == 0){
            LOGGER.error("No " + tagName + " tag found in XML.");
        } else if (sieNodeList.getLength()>1){
            LOGGER.error("More than one " + tagName + " tag value found in XML");
        } else {
            Element sieElem = (Element) sieNodeList.item(0);
            sieElem.setAttribute("translate", "no");
        }
        
    }

    private static boolean uploadOrSave(EPMDocument sie, File modFile, boolean upload, String directory) throws WTException, IOException{
        boolean success = true;
        if (upload && modFile != null) {
            Transaction transaction = new Transaction();
            try {
                File checkmodefile = new File(directory + "/" + sie.getNumber() + "_mod.xml");
                if (checkmodefile.exists()) {
                    checkmodefile.delete();
                }
                FileUtils.copyFile(modFile, new File(directory + "/" + sie.getNumber() + "_mod.xml"));
                transaction.start();
                uploadContent(sie, modFile, sie.getCADName());
                transaction.commit();
                transaction = null;

            } catch (PersistenceException e) {
                LOGGER.error(e.getMessage());
                throw new WTException(e);
            } catch (WTException e) {
                LOGGER.error(e.getMessage());
                throw new WTException(e);
            } finally {
                if (transaction != null) {
                    LOGGER.info("       -Correction unsuccessful");
                    transaction.rollback();
                    success = false;
                } else {
                    LOGGER.info("       -Correction successful");
                }
            }
        } else if (!upload && modFile != null) {
            try {
                File checkmodefile = new File(
                        directory + "/" + sie.getNumber() + "_" + sie.getVersionDisplayIdentifier() + "_mod.xml");
                if (checkmodefile.exists()) {
                    checkmodefile.delete();
                }
                FileUtils.moveFile(modFile, new File(
                        directory + "/" + sie.getNumber() + "_" + sie.getVersionDisplayIdentifier() + "_mod.xml"));
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
                throw new WTException(e);
            }
        }
        return success;
    }

    private static void uploadContent(EPMDocument aEPMDocument, final File file, final String filename)
            throws WTException {
        LOGGER.trace("uploadContent - start");

        final String contentCategory = MediaUtils.getPrimaryContent(aEPMDocument).getCategory();
        ApplicationData appData = null;
        FileInputStream stream = null;
        try {
            // Creating new ApplicationData to upload new content
            appData = ApplicationData.newApplicationData(aEPMDocument);
            appData.setFileSize(file.length());
            appData.setFileName(filename);
            appData.setUploadedFromPath(file.getPath());
            appData.setRole(ContentRoleType.PRIMARY);
            appData.setCategory(contentCategory);

            // Force delete actual content (only one primary content allowed)
            ContentServerHelper.service.deleteContent(aEPMDocument, MediaUtils.getPrimaryContent(aEPMDocument));
            stream = new FileInputStream(file);
            // Upload new content
            ContentServerHelper.service.updateContent(aEPMDocument, appData, stream);

        } catch (FileNotFoundException e) {
            LOGGER.error(e.getMessage());
            throw new WTException(e);
        } catch (PropertyVetoException e) {
            LOGGER.error(e.getMessage());
            throw new WTException(e);
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            throw new WTException(e);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    LOGGER.error(e);
                }
            }
            if (file != null && file.exists()) {
                file.delete();
            }
        }

        LOGGER.trace("uploadContent - end");
    }

    private static Map<String, String> getOutputProperties() {
        Map<String, String> outputProperties = new HashMap<>();

        // svg:style nodes should contain only CDATA children, and no Text
        // node
        outputProperties.put(OutputKeys.OMIT_XML_DECLARATION, "no");
        outputProperties.put(OutputKeys.ENCODING, "UTF-8");
        outputProperties.put(OutputKeys.INDENT, "no");
        return outputProperties;
    }

    /**
     * Gets the output option.
     *
     * @param commandLine
     *            the command line
     * @return the output option
     * @throws ParseException
     *             the parse exception
     */
    private static String getOutputOption(CommandLine commandLine) throws ParseException {
        String output = null;
        if (commandLine.hasOption(OUTPUT_ARG)) {
            output = commandLine.getOptionValue(OUTPUT_ARG);
        } else {
            throw new ParseException(OUTPUT_ARG + " argument has not been declared");
        }
        return output;
    }

    private static String addDateToBackupDir(String backupDir){
        Timestamp time = new Timestamp(System.currentTimeMillis());
        String formatTime=time.toString().substring(0,time.toString().lastIndexOf(":")).replace(" ", "_");
        if (run){
            backupDir = backupDir + "/" + MODE_RECOVERY + "-" + formatTime;
        } else {
            backupDir = backupDir + "/" + MODE_DRY + "-" + formatTime;
        }

        return backupDir;
    }
    
    /**
     * Get the translations of a source SIE
     * @param document
     * @return
     * @throws WTException
     */
    public static Set<EPMDocument> getTranslations(EPMDocument document) throws WTException {
        Set<EPMDocument> translations = new HashSet<>();
        QueryResult qr = PersistenceHelper.manager.navigate(document, TranslationLink.ROLE_BOBJECT_ROLE, TranslationLink.class, false);

        while (qr.hasMoreElements()) {
            final TranslationLink translationLink = (TranslationLink) qr.nextElement();
            EPMDocument translatedDocument = (EPMDocument) translationLink.getOtherObject(document);
            if (translatedDocument.isLatestIteration() && !translations.contains(translatedDocument)) {
                translations.add(translatedDocument);
            }
        }
        return translations;
    }

    /**
     * Retrieve the latest version iteration of a Versionable
     * @param object
     * @return
     * @throws WTException
     */
    private static Versionable getLatestVersionIteration(Versionable rc) throws WTException {
        QueryResult versionQR = VersionControlHelper.service.allVersionsOf(rc.getMaster());
        return (Versionable) versionQR.nextElement();
    }

    /**
     * Gets the log level option.
     *
     * @param commandLine
     *            the command line
     * @return the log level option
     * @throws ParseException
     *             the parse exception
     */
    private static String getLogLevelOption(CommandLine commandLine) throws ParseException {
        if (!commandLine.hasOption(LONG_LOG_LEVEL_ARG)) {
            throw new ParseException(LONG_LOG_LEVEL_ARG + " argument has not been declared");
        }

        String logLevelOption = commandLine.getOptionValue(LONG_LOG_LEVEL_ARG);
        if (LOG_LEVELS.contains(logLevelOption)) {
            return commandLine.getOptionValue(LONG_LOG_LEVEL_ARG);
        }
        throw new IllegalArgumentException("Log level should be: " + Arrays.toString(LOG_LEVELS.toArray()));
    }

    public static Map<TimeUnit, Long> computeDiff(Date start, Date end) {
        long diffInMilliSeconds = end.getTime() - start.getTime();
        List<TimeUnit> timeUnits = new ArrayList<>(EnumSet.of(TimeUnit.DAYS, TimeUnit.HOURS, TimeUnit.MINUTES, TimeUnit.SECONDS));
        Collections.reverse(timeUnits);
        Map<TimeUnit, Long> result = new LinkedHashMap<>();
        long milliSecondsRest = diffInMilliSeconds;
        for (TimeUnit unit : timeUnits) {
            long diff = unit.convert(milliSecondsRest, TimeUnit.MILLISECONDS);
            long diffInMilliSecondsForUnit = unit.toMillis(diff);
            milliSecondsRest = milliSecondsRest - diffInMilliSecondsForUnit;
            result.put(unit, diff);
        }
        return result;
    }

    /**
     * Gets the input option.
     *
     * @param commandLine
     *            the command line
     * @return the input option
     * @throws ParseException
     *             the parse exception
     */
    private static String getInputOption(CommandLine commandLine) throws ParseException {
        String input = null;
        if (commandLine.hasOption(INPUT_ARG)) {
            input = commandLine.getOptionValue(INPUT_ARG);
        } else {
            throw new ParseException(INPUT_ARG + " argument has not been declared");
        }
        return input;
    }

    /**
     * Gets the number range option.
     * 
     * @param commandLine
     * @return the number range option
     * @throws ParseException
     */
    private static String getNumberRangeOption(CommandLine commandLine) throws ParseException {
        String numberRange = null;
        if (commandLine.hasOption(NUMBER_RANGE)) {
            numberRange = commandLine.getOptionValue(NUMBER_RANGE);
        }
        return numberRange;
    }

    /**
     * Gets the backup dir option.
     * 
     * @param commandLine
     * @return the backup dir option
     * @throws ParseException
     */
    private static String getBackupOption(CommandLine commandLine) throws ParseException {
        String backup = null;
        if (commandLine.hasOption(BACKUP_ARG)) {
            backup = commandLine.getOptionValue(BACKUP_ARG);
        }
        return backup;
    }

    /**
     * Gets the csv rows limit option
     *
     * @param commandLine
     *            the command line
     * @return the csv limit option
     * @throws ParseException
     *             the parse exception
     */
    private static long getCsvLimitOption(CommandLine commandLine) {
        if (!commandLine.hasOption(CSV_LIMIT_ARG)) {
            return -1L;
        } else {
            String csvLimitOption = commandLine.getOptionValue(CSV_LIMIT_ARG);
            try {
                return Long.parseLong(csvLimitOption);
            } catch (NumberFormatException nfe) {
                LOGGER.error(nfe.getLocalizedMessage(), nfe);
                throw new IllegalArgumentException("CSV file rows limit should be an integer ex 100000");
            }
        }

    }

    /**
     * Gets the type option
     * @param commandLine
     * @return the type option
     * @throws ParseException
     */
    private static SieTranslationProcessType getTypeOption(CommandLine commandLine) throws ParseException {
        // get and check type value from commandline
        String type = null;
        SieTranslationProcessType partType = null;

        if (commandLine.hasOption(OBJECT_TYPE_ARG)) {
            type = commandLine.getOptionValue(OBJECT_TYPE_ARG);
        } else {
            throw new ParseException(OBJECT_TYPE_ARG + " argument has not been declared");
        }

        partType = SieTranslationProcessType.valueOf(type);
        LOGGER.info("Type : " + partType.getName());

        return partType;
    }

    /**
     * Acquire server.
     *
     * @return the remote method server
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private static RemoteMethodServer acquireServer() throws IOException {
        LOGGER.info("Acquiring invocation target service instance");
        String urlServer = WTProperties.getLocalProperties().getProperty(WT_SERVER_CODEBASE) + "/";
        LOGGER.info("URL Server: " + urlServer);

        return RemoteMethodServer.getInstance(new URL(urlServer));
    }

    /**
     * Gets the org id.
     *
     * @param orgName
     *            the org name
     * @return the org id
     * @throws WTException
     *             the WT exception
     */
    private static long getOrgId(String orgName) throws WTException {
        orgName = orgName.substring(0, 1).toUpperCase() + orgName.substring(1).toLowerCase();
        WTOrganization organization = OrganizationHelper.getOrganizationByName(orgName);
        return organization.getPersistInfo().getObjectIdentifier().getId();
    }    
    /**
     * Login command.
     *
     * @param commandLine
     *            the command line
     * @return the authorization
     * @throws ParseException
     *             the parse exception
     */
    private static Authorization loginCommand(CommandLine commandLine) {
        final Options posixOptions = getOptions();

        HelpFormatter formatter = new HelpFormatter();

        String user = null;
        String passw = null;

        if (commandLine.hasOption(HELP_ARG)) {
            formatter.printHelp(USAGE, posixOptions);
            System.exit(0);
        }
        if (commandLine.hasOption(USER_ARG)) {
            user = commandLine.getOptionValue(USER_ARG);
        }
        if (commandLine.hasOption(PASSW_ARG)) {
            passw = commandLine.getOptionValue(PASSW_ARG);
        }

        if (user != null || passw != null) {
            return new Authorization(user, passw);
        } else {
            throw new IllegalArgumentException("Values of user and password are empty or null");
        }
    }

    /**
     * Gets the command line from args. If start from windchill shell, modify
     * options according MODE and parse again the args. if in method server,
     * only one parse is done
     *
     * @param args
     *            the args
     * @return the command line from args
     * @throws ParseException
     *             the parse exception
     */
    private static CommandLine getCommandLineFromArgs(String[] args) throws ParseException {
        final CommandLineParser cmdLinePosixParser = new PosixParser();
        Options posixOptions = getOptions();

        LOGGER.info("CommandLine args: " + Arrays.asList(args));
        CommandLine commandLine = cmdLinePosixParser.parse(posixOptions, args);
        if (!RemoteMethodServer.ServerFlag) {
            posixOptions = configureCommandLine(commandLine);
            commandLine = cmdLinePosixParser.parse(posixOptions, args);
        }
        return commandLine;

    }

    /**
     * Configure command line.
     *
     * @param commandLine
     *            the command line
     * @return the options
     */
    private static Options configureCommandLine(CommandLine commandLine) {

        Options result = getOptions();
        String modeOption = commandLine.getOptionValue(MODE_ARG);
        if ("DRY".equals(modeOption)) {
            Option opt = result.getOption(OUTPUT_ARG);
            opt.setRequired(true);
            result.addOption(opt);
        }
        if ("RECOVERY".equals(modeOption)) {
            Option opt = result.getOption(INPUT_ARG);
            opt.setRequired(true);
            result.addOption(opt);
        }
        return result;
    }

    /**
     * Gets the options.
     *
     * @return the options
     */
    private static Options getOptions() {
        final Options posixOptions = new Options();

        Option helpOption = new Option(HELP_ARG, false, "Help");
        posixOptions.addOption(helpOption);

        Option userOption = new Option(USER_ARG, true, "User name (ex: wcadmin)");
        userOption.setRequired(true);
        posixOptions.addOption(userOption);

        Option passwOption = new Option(PASSW_ARG, true, "User password (ex: wcadmin)");
        passwOption.setRequired(true);
        posixOptions.addOption(passwOption);

        Option modeOption = new Option(MODE_ARG, true, "Execution mode : " + Arrays.toString(ExecutionMode.values()));
        modeOption.setRequired(true);
        posixOptions.addOption(modeOption);

        Option outputOption = new Option(OUTPUT_ARG, true, "Output CSV file");
        posixOptions.addOption(outputOption);

        Option inputOption = new Option(INPUT_ARG, true, "Input CSV file");
        posixOptions.addOption(inputOption);

        Option backupOption = new Option(BACKUP_ARG, true, "Backup directory for XML files");
        posixOptions.addOption(backupOption);

        Option typeOption = new Option(OBJECT_TYPE_ARG, true, "SIE type : " + Arrays.toString(SieTranslationProcessType.values()));
        posixOptions.addOption(typeOption);

        Option orgOption = new Option(ORG_ARG, true, "Organization Name (ex: Renault, Nissan). All organization is the default value");
        posixOptions.addOption(orgOption);

        Option numberRangeOption = new Option(NUMBER_RANGE, true, "Number range (ex: 5-20)");
        posixOptions.addOption(numberRangeOption);

        Option logLevelOption = new Option(null, LONG_LOG_LEVEL_ARG, true, "Log levels: " + Arrays.toString(LOG_LEVELS.toArray()));
        posixOptions.addOption(logLevelOption);

        Option csvLimitOption= new Option(null, CSV_LIMIT_ARG, true, "Input CSV file rows limit");
        posixOptions.addOption(csvLimitOption);

        return posixOptions;
    }
}
