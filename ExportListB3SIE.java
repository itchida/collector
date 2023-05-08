package ext.renault.phenix.datafix.exportListB3SIE;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPath;
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
import org.apache.commons.csv.CSVPrinter;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import ext.renault.phenix.interfaces.exportmd.repository.manager.Authorization;
import wt.load.LoadServerHelper;
import wt.log4j.LogR;
import wt.method.RemoteAccess;
import wt.method.RemoteMethodServer;
import wt.session.SessionHelper;
import wt.util.WTException;
import wt.util.WTProperties;

public class ExportListB3SIE implements RemoteAccess {
	
	private static final Logger LOGGER = LogR.getLogger(ExportListB3SIE.class.getName());
	private static final XPath xpath = XPathFactory.newInstance().newXPath();
	private static final int MAX_LINE = 100000;
	
	private static final String EXPRESSION = "/servinfo/@*";
	
	/** The Constant Contains OUTPUT_ARG. */
    private static final String OUTPUT_ARG = "o";

    /** The Constant Contains INPUT_ARG. */
    private static final String INPUT_ARG = "i";
	
    /** The Constant Contains Latest Version. */
    private static final String LATEST_VERSION_ARG = "latestVersion";
    
    /** The Constant Contains LANGUE ARG. */
    private static final String LANG_ARG = "lang";
    
    /** The Constant Contains Ignored Types. */
    private static final String IGNORE_TYPE_ARG = "ignoreType";
    
    /** The Constant Contains Exported Types. */
    private static final String EXPORT_TYPE_ARG = "exportType";
    
    private static final String CLASS_SIMPLE_NAME = ExportListB3SIE.class.getSimpleName();
    
    private static final String PRINT_MESSAGE = "SIE Stored";
    
    /** The Constant CLASS_NAME. */
    private static final String CLASS_NAME = ExportListB3SIE.class.getName();
    
    /** The Constant USAGE. */
    private static final String USAGE = "\nwindchill " + CLASS_NAME;
    
    /** The Constant WT_SERVER_CODEBASE. */
    private static final String WT_SERVER_CODEBASE = "wt.server.codebase";
    
    /** The Constant METHOD. */
    private static final String METHOD = "main";
    
    /** The Constant USER_ARG. */
    private static final String USER_ARG = "u";

    /** The Constant PASSW_ARG. */
    private static final String PASSW_ARG = "p";
    
    // Main Method
    
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
           LOGGER.info("############################################################");
           LOGGER.info("Starting SIE stored.");
  		try {
  			runExport(commandLine);
  			System.out.println("Export Successfully at " + commandLine.getOptionValue(OUTPUT_ARG));
          } catch (Exception e) {
              LOGGER.debug(e, e);
              LOGGER.error("Problem during runExport.", e);
              LoadServerHelper.printMessage(PRINT_MESSAGE + " ended with errors.");
          } 
        	  finally {
              LOGGER.info("Ending SIE Stored.");
              LOGGER.info("############################################################");
              LOGGER.removeAppender(appender);
          }
        } else {
            //validate the commande line
            CommandLine commandLine = null;
            try {
                commandLine = getCommandLineFromArgs(args);
            } catch (Exception e) {
                System.err.println(e.getMessage());
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
	
	private static RemoteMethodServer acquireServer() throws IOException {
        LOGGER.info("Acquiring invocation target service instance");
        String urlServer = WTProperties.getLocalProperties().getProperty(WT_SERVER_CODEBASE) + "/";
        LOGGER.info("URL Server: " + urlServer);

        return RemoteMethodServer.getInstance(new URL(urlServer));
    }

	// this Method to get Commandeline from ARGS
	private static CommandLine getCommandLineFromArgs(String[] args) throws ParseException {
        final CommandLineParser cmdLinePosixParser = new PosixParser();
        Options posixOptions = getOptions();
        
        LOGGER.info("CommandLine args: " + Arrays.asList(args));
        CommandLine commandLine = cmdLinePosixParser.parse(posixOptions, args);
 
        commandLine = cmdLinePosixParser.parse(posixOptions, args);
        return commandLine;
    }
	
	// this Method to get Options from commandLine
	private static Options getOptions() {
        final Options posixOptions = new Options();

        Option outputOption = new Option(OUTPUT_ARG, true, "Output PATH");
        outputOption.setRequired(true);
        posixOptions.addOption(outputOption);

        Option inputOption = new Option(INPUT_ARG, true, "Input PATH");
        inputOption.setRequired(true);
        posixOptions.addOption(inputOption);

        Option userOption = new Option(USER_ARG, true, "User name (ex: wcadmin)");
        userOption.setRequired(true);
        posixOptions.addOption(userOption);
        
        Option passwOption = new Option(PASSW_ARG, true, "User password (ex: wcadmin)");
        passwOption.setRequired(true);
        posixOptions.addOption(passwOption);
        
        Option latestVersion = new Option(LATEST_VERSION_ARG, true, "latest Version");
        posixOptions.addOption(latestVersion);
        
        Option lang = new Option(LANG_ARG, true, "Exported Languages");
        posixOptions.addOption(lang);

        Option ignoreType = new Option(IGNORE_TYPE_ARG, true, "ignored types");
        posixOptions.addOption(ignoreType);
        
        
        Option type = new Option(EXPORT_TYPE_ARG, true, "Export types");
        posixOptions.addOption(type);
        
        
        return posixOptions;
	}
	
	//Check Path 
	private static void checkPath(String path,String type) {
		Path pt = Paths.get(path);
        if (!Files.exists(pt)) {
        	System.err.println(type+" Path does not exist , Choose a exists PATH.");
        	LOGGER.error(type+" Path does not exist , Choose a exists PATH.");
        	System.exit(1);
        }
	}
	
	
	// This Method who run our Export 
	private static void runExport(CommandLine commandLine) throws WTException, ParseException, XPathExpressionException, ParserConfigurationException, SAXException, IOException, TransformerException {
		
        String outputOption = getOutputOption(commandLine);
        checkPath(outputOption,"Output");
        String inputOption = getInputOption(commandLine);
        checkPath(inputOption,"Input");
        List<String> langs = getLangs(commandLine);
        boolean latest = getLatestVersion(commandLine);
        List<String> exportTypes = getExportType(commandLine);
        List<String> ignoredTypes = ignoredtType(commandLine);

        IteratelistFiles(outputOption,langs,latest,exportTypes,ignoredTypes,inputOption);

    }
	
	// To Get Latest version from commandLine
	private static boolean getLatestVersion(CommandLine commandLine) throws ParseException {
		boolean latest=false; 
        if (commandLine.hasOption(LATEST_VERSION_ARG)) {
        	if("true".equals(commandLine.getOptionValue(LATEST_VERSION_ARG))) {
        		latest = true;
        	}
        }
        return latest;
    }

	// To Get Langues from commandLine
	private static List<String> getLangs(CommandLine commandLine) throws ParseException {
        List<String> langs = null; 
        if (commandLine.hasOption(LANG_ARG)) {
           String str = commandLine.getOptionValue(LANG_ARG);
           if (str != null && str.indexOf(',') != -1) {
        	   langs = Arrays.asList(str.split(","));
           }
           else  {
        	   langs = new ArrayList<String>();
        	   langs.add(str);
           }
        }
        return langs;
    }
	
	// To Get Output PATH from commandLine
	private static String getOutputOption(CommandLine commandLine) throws ParseException {
        String output = null;
        if (commandLine.hasOption(OUTPUT_ARG)) {
            output = commandLine.getOptionValue(OUTPUT_ARG);
        }
        return output;
    }
	
	// To Get Iput PATH from commandLine
		private static String getInputOption(CommandLine commandLine) throws ParseException {
	        String input = null;
	        if (commandLine.hasOption(INPUT_ARG)) {
	            input = commandLine.getOptionValue(INPUT_ARG);
	        }
	        return input;
	    }
	
	//To Get List of Export Types
	private static List<String> getExportType(CommandLine commandLine) throws ParseException {
		List<String> exportTypes = null;
        if (commandLine.hasOption(EXPORT_TYPE_ARG)) {
        	String str = commandLine.getOptionValue(EXPORT_TYPE_ARG);
        	if (str != null && str.indexOf(',') != -1) {
        		exportTypes = Arrays.asList(str.split(","));        		
        	}
        	else {
        		exportTypes = new ArrayList<String>();
        		exportTypes.add(str);
        	}
        }
        return exportTypes;
    }
	
	//To Get List of Ignored Types
	private static List<String> ignoredtType(CommandLine commandLine) throws ParseException {
		List<String> ignoredtType = null;
        if (commandLine.hasOption(IGNORE_TYPE_ARG)) {
        	String str = commandLine.getOptionValue(IGNORE_TYPE_ARG);
        	if (str != null && str.indexOf(',') != -1) {
        		ignoredtType = Arrays.asList(str.split(","));
        	}
        	else {
        		ignoredtType = new ArrayList<String>();
        		ignoredtType.add(str);
        	}
        }
        return ignoredtType;
    }
	
	//To get only XML files
	private static File[] filterFiles(String path) {
		File directory = new File(path);
		File[] files = directory.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				// TODO Auto-generated method stub
				return name.endsWith(".xml");
			}
		});
		return files;
	}
	
	// Get list of all the folders in the directory
	private static File[] IteratelistFolders(String directoryPath) {
		File directory = new File(directoryPath);
        File[] folders = directory.listFiles(new FileFilter() {
			
			@Override
			public boolean accept(File file) {
				// TODO Auto-generated method stub
				return file.isDirectory();
			}
        });
		return folders;
	}
	
	private static void IteratelistFiles(String outputOption,List<String> langs,boolean latest,List<String> exportTypes,List<String> ignoredTypes,String inputPath) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException, TransformerException, WTException {
		File[] folders = IteratelistFolders(inputPath);
		// Map<String, String> sieItem = new HashMap<String, String>();
		for (File folder : folders) {
			//to check if this language in Languages list
			if (langs != null) {
				if (!langs.contains(folder.getName())) {
					continue;
				}
			}
			File[] files = filterFiles(inputPath+File.separator+folder.getName()+File.separator);
			List<SieRecord> listOfSIE = new ArrayList<>();
			Map<String, SieRecord> latestList = new HashMap<>();
			for (File file : files) {
				SieRecord sieItem = xmlReader(file);
				//to check if this Type in Ignoredlist
				if (ignoredTypes != null) {
					if (ignoredTypes.contains(sieItem.getType())) {
						continue;
					}
				}
				//to check if this Type in Exportlist
				if (exportTypes != null) {
					if (!exportTypes.contains(sieItem.getType())) {
						continue;
					}
				}
				if (latest) {
					if (latestList.get(sieItem.getId()) == null){ 
						latestList.put(sieItem.getId(), sieItem); 
					}
					else{ 
						final SieRecord tmpSIE = latestList.get(sieItem.getId());
						if (tmpSIE != null && sieItem != null) {
							if (tmpSIE.getId().compareTo(sieItem.getVersion()) < 0) {
								latestList.put(sieItem.getId(), sieItem);
							}
						}
					}
				}
				else {
					listOfSIE.add(sieItem);
				}
			}
			csvPrinter(listOfSIE,folder.getName(),latestList,latest,outputOption);
		}
	}
	
	interface Mapper<T, R> {
		R map(T item);
	}
	
	private static boolean createFolder(String folderPath) {
		File folder = new File(folderPath);
		boolean success = folder.mkdirs();
        if (success) {
        	LOGGER.info("Folder created successfully at : "+folderPath);
        	return true;
        } else {
        	LOGGER.info("Failed to create folder at "+folderPath);
        	return false;
        }
	}
	
	private static <T, C extends Collection<T>> void csvPrinterProceed(String outputPath,String lang, C collection, Mapper<T, SieRecord> mapper) {
		
		try {
			int count = 1;
			int nbr = 1;
			if (!collection.isEmpty()) {
				CSVFormat format = CSVFormat.DEFAULT.withHeader("NUMBER", "VERSION","LAST MODIFICATION","STATUS","APPLICABILITY").withDelimiter(';');
				String dateString = new SimpleDateFormat("yyMMdd_hh_mm_ss").format(new Date());
				String folderName = "ExportMDRepository_"+lang+"_"+dateString;
				String folderPath = outputPath+File.separator+folderName;
				if (!createFolder(folderPath)) {
					return;
				}
				CSVPrinter printer = new CSVPrinter(new FileWriter(folderPath+File.separator+"sie_"+lang+"_"+nbr+"_"+dateString+".csv"), format);
				
				for (final T value : collection) {
					final SieRecord sieItem = mapper.map(value);
					if ( count > MAX_LINE) {
						count = 1;
						nbr++;
						printer.close();
						printer = new CSVPrinter(new FileWriter(folderPath+File.separator+"sie_"+lang+"_"+nbr+"_"+dateString+".csv"), format);
					}
					count++;
					printer.printRecord(sieItem.getId(),sieItem.getVersion(),sieItem.getLastModification(),sieItem.getStatus(),sieItem.getApplicability());
				}
				printer.close();
			}
		
		} catch (IOException e) {
            LOGGER.error("An error occurred !!\n"+e.getMessage());
        }
	}
	
	private static void csvPrinter(List<SieRecord> listOfSIE,String lang,Map<String, SieRecord> latestList,boolean latest,String outputPath) throws IOException {
		
		if (latest) {
			csvPrinterProceed(outputPath,lang, latestList.entrySet(), new Mapper<Map.Entry<String,SieRecord>, SieRecord>() {
				@Override
				public SieRecord map(Entry<String, SieRecord> item) {
					return item.getValue();
				}
			});
		} else {
			csvPrinterProceed(outputPath,lang, listOfSIE, new Mapper<SieRecord, SieRecord>() {
				@Override
				public SieRecord map(SieRecord item) {
					return item;
				}
			});
		}
	}
	
	//Method who read XML FILES
	private static SieRecord xmlReader(File file) throws SAXException, IOException, ParserConfigurationException, XPathExpressionException {
		
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		Document doc = factory.newDocumentBuilder().parse(file);
	    XPathExpression expr = xpath.compile(EXPRESSION);
	    NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
	    if ( nodes == null) {
	    	return null;
	    }

	    String id = null;
		String version = null;
		String type = null;
		String applicability = null;
		String lastModification = null;
		String status = null;
	    
	    for (int i = 0; i < nodes.getLength(); i++) {
	    	Node attribute = nodes.item(i);
	    	if ("id".equals(attribute.getNodeName())) {
	    		if (attribute.getNodeValue() != null && attribute.getNodeValue() != "") {
		    		String[] strArray = attribute.getNodeValue().split(",");
		    		if (strArray.length > 1) {
			    		id = strArray[0];
			    		version = strArray[1].substring(0, 1);
		    		}
	    		}
	    	}
	    	else if ("status".equals(attribute.getNodeName())) {
	    		status = attribute.getNodeValue();
	    	}
	    	else if ("last-mod-date".equals(attribute.getNodeName())) {
	    		lastModification = attribute.getNodeValue();
	    	}
	    	else if ("sieconfigid".equals(attribute.getNodeName())) {
	    		applicability = attribute.getNodeValue();
	    	}
	    	else if ("SIEType".equals(attribute.getNodeName())) {
	    		String sie_type = attribute.getNodeValue();
	    		if (sie_type != null && sie_type.indexOf('-') != -1) {
	    			String[] parts = sie_type.split("-");
	    			sie_type = parts[0];
	    		}
	    		type = sie_type;
	    	}
	      }
	    return new SieRecord(id, version, type, applicability, lastModification, status);
	}
	
}
