private static void runDeleteForGies(CommandLine commandLine, String outputOption, GieCachedDataInitializationType type, long csvRowsLimit) throws WTException {
        ExportListWriter outputWriter = null;
        File outputFile = new File(outputOption);
        outputFile.getParentFile().mkdirs();

        try (FileOutputStream os = new FileOutputStream(outputFile);
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(os, CSV_ENCODING), ExportListWriter.BUFFER_SIZE);) {
            outputWriter = ExportListWriterFactory.getExportWriter(CSV_OUPUT_TYPE);
            outputWriter.setFileName(outputFile.getName());
            outputWriter.setOut(out);
            outputWriter.writeColumnHeaders(CSV_OUTPUT_HEADER_FOR_GIE_CACHED_DATA, CSV_SEPARATOR);

            Long currentFileLines = Long.valueOf(0);

            // Gets all Source GIE of type 'type'
            List<WTPart> giesSet;
            if(type.name().equals("ALL")) {
                giesSet = new ArrayList<>();
                for(GieCachedDataInitializationType gieType : GieCachedDataInitializationType.values()) {
                    if(!gieType.name().equals("ALL")){
                        giesSet.addAll(getAllSourceGie(gieType));
                    }
                }
            }else {
                giesSet = getAllSourceGie(type);
            }

            // Deletes all the links for the gies and fill the csv file
            for (WTPart gie : giesSet) {
                List<Persistable> objects = GieCachedDataHelper.service.getAllObjectsRelatedToGIE(gie);
                for(Persistable object : objects) {
                    String objectName = "";
                    String objectNumber = "";
                    String gieVersion = "";
                    if(object instanceof WTPartMaster){
                        WTPartMaster partMaster = (WTPartMaster) object;
                        objectName = partMaster.getName();
                        objectNumber = partMaster.getNumber();
                    }else if(object instanceof EPMDocumentMaster){
                        EPMDocumentMaster epmdocMaster = (EPMDocumentMaster) object;
                        objectName = epmdocMaster.getName();
                        objectNumber = epmdocMaster.getNumber();
                    }
                    gieVersion = gie.getVersionInfo().getIdentifier().getValue() + "." + gie.getIterationInfo().getIdentifier().getValue();
                    GIEData gieData = new GIEData(REFERENCE_FACTORY.getReference(gie), gie.getName(), gie.getNumber(), gieVersion, GieCachedDataHelper.service.getGieType(gie));
                    ObjectData objectData = new ObjectData(REFERENCE_FACTORY.getReference(object), objectName, objectNumber,GieCachedDataHelper.service.getObjectType(object).name());
                    ConfigurableReferenceLink link = StandardGieCachedDataService.getConfigurableReferenceLink(gie,SoftTypes.GIE_OBJECT_LINK_TYPE,objectNumber);
                    // Get count and parent Elements
                    String count = (String) PhenixIBAUtils.getIBAvalue(link, SoftAttributes.COUNT_ATTRIBUTE);
                    String parentElementStr = "";
                    Object parentElement = PhenixIBAUtils.getIBAvalue(link,SoftAttributes.PARENT_ELEMENT_ATTRIBUTE);
                    if(parentElement instanceof String) {
                        parentElementStr = (String) parentElement;
                    }else if(parentElement instanceof Object[]) {
                        Object[] parentElementTab = (Object[]) parentElement;
                        for(Object parentElementObject : parentElementTab) {
                            parentElementStr += ((String) parentElementObject) + ", ";
                        }
                        parentElementStr = parentElementStr.substring(0, parentElementStr.length() - 2);
                    }
                    GieCachedDataInitializationRow csvRow = new GieCachedDataInitializationRow(gieData,objectData,count,parentElementStr);
                    currentFileLines++;
                    csvRow.setProcessed();
                    List<Object> writerValues = writeRowData(csvRow.getLineRow(), outputWriter, outputFile, currentFileLines, outputOption, csvRowsLimit, false);
                    currentFileLines = (Long) writerValues.get(0);
                    outputWriter = (ExportListWriter) writerValues.get(1);
                    outputOption = (String) writerValues.get(2);
                    outputFile = (File) writerValues.get(3);
                    deleteLink(link);
                }
            }


            LOGGER.info("Writing output file: {}" , outputFile.getAbsolutePath());
            
            outputWriter.close();

        } catch (Exception e) {
            throw new WTException(e, "An error occured while trying to create output report file.");
        }
    }


    public static void main(String[] args) throws Exception {

        if (RemoteMethodServer.ServerFlag) {
            SessionHelper.manager.getPrincipal();
            CommandLine commandLine = getCommandLineFromArgs(args);
            String logdir = WTProperties.getServerProperties().getProperty("wt.logs.dir");
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configuration configuration = context.getConfiguration();
            PatternLayout layout = PatternLayout.newBuilder().withConfiguration(configuration)
                    .withPattern("%d{ISO8601} %-5p [%t] - %m%n").build();
            LoggerConfig loggerConfig = configuration.getLoggerConfig(LogManager.getLogger().getName());

            // PatternLayout layout = new PatternLayout("%d{ISO8601} %-5p [%t] - %m%n");
            String dateString = new SimpleDateFormat("yyMMdd_hh_mm").format(new Date());
            String path = new StringBuilder(logdir).append(File.separator).append("datafix").append(File.separator)
                    .append(CLASS_SIMPLE_NAME).append(File.separator).append(CLASS_SIMPLE_NAME).append("_")
                    .append(dateString).append(".log").toString();
            // FileAppender appender = new FileAppender(layout, path, true);
            FileAppender appender = FileAppender.newBuilder().setName(CLASS_SIMPLE_NAME).setLayout(layout).withFileName(path).build();
            /*
             * appender.setImmediateFlush(true); appender.activateOptions();
             * appender.setName(CLASS_SIMPLE_NAME);
             */
            configuration.addAppender(appender);
            loggerConfig.addAppender(appender, Level.ALL, null);
            context.updateLoggers();
            if (configuration.getAppender(CLASS_SIMPLE_NAME) != null) {
                configuration.removeLogger(CLASS_SIMPLE_NAME);
            }
            configuration.addAppender(appender);
            LOGGER.atLevel(Level.ALL);
            LOGGER.info(LOG_MESSAGE);
            LOGGER.info(SEPARATOR);
            LOGGER.info("Starting {}" , CLASS_SIMPLE_NAME);
            LOGGER.info(LOG_MESSAGE);

            //Updated for IRN59102-18174 - END       
            
            /*
             * PatternLayout layout = new PatternLayout("%d{ISO8601} %-5p [%t] - %m%n"); String dateString = new
             * SimpleDateFormat("yyMMdd_hh_mm").format(new Date()); String path = new
             * StringBuilder(logdir).append(File.separator).append("datafix").append(File.separator).append(
             * CLASS_SIMPLE_NAME).append(File.separator).append(CLASS_SIMPLE_NAME).append("_").append(dateString).append
             * (".log").toString(); FileAppender appender = new FileAppender(layout, path, true);
             * appender.setImmediateFlush(true); appender.activateOptions(); appender.setName(CLASS_SIMPLE_NAME);
             * LOGGER.addAppender(appender); LOGGER.setLevel(Level.toLevel(getLogLevelOption(commandLine), Level.ALL));
             */
            LOGGER.info(SEPARATOR);
            LOGGER.info("Starting GIE cached data initialization Tool.");
            LOGGER.info(LOG_MESSAGE);

            try {
                runMode(commandLine);
            } catch (Exception e) {
                LOGGER.debug(e, e);
                LOGGER.error("Problem during runMode.", e);
                LoadServerHelper.printMessage(PRINT_MESSAGE + " ended with errors.");
            } finally {
                LOGGER.info("Ending GIE cached data initialization Tool.");
                LOGGER.info(SEPARATOR);
                context = (LoggerContext) LogManager.getContext(false);
                configuration = context.getConfiguration();
                loggerConfig = configuration.getLoggerConfig(LogManager.getLogger().getName());
                loggerConfig.removeAppender(CLASS_SIMPLE_NAME);
                 if (configuration.getAppender(CLASS_SIMPLE_NAME) != null) {
                    configuration.removeLogger(CLASS_SIMPLE_NAME);
                }
            }

        } else {
            //validate the commande line
            CommandLine commandLine = null;
            try {
                commandLine = getCommandLineFromArgs(args);
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
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

    private static void runInitForGies(CommandLine commandLine, String outputOption, GieCachedDataInitializationType type, long csvRowsLimit) throws WTException {
        ExportListWriter outputWriter = null;
        File outputFile = new File(outputOption);
        outputFile.getParentFile().mkdirs();

        try (FileOutputStream os = new FileOutputStream(outputFile);
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(os, CSV_ENCODING), ExportListWriter.BUFFER_SIZE);) {
            outputWriter = ExportListWriterFactory.getExportWriter(CSV_OUPUT_TYPE);
            outputWriter.setFileName(outputFile.getName());
            outputWriter.setOut(out);
            outputWriter.writeColumnHeaders(CSV_OUTPUT_HEADER_FOR_GIE_CACHED_DATA, CSV_SEPARATOR);

            Long currentFileLines = Long.valueOf(0);

            // Gets all Source GIE of type 'type'
            List<WTPart> giesSet;
            if(type.name().equals("ALL")) {
                giesSet = new ArrayList<>();
                for(GieCachedDataInitializationType gieType : GieCachedDataInitializationType.values()) {
                    if(!gieType.name().equals("ALL")){
                        giesSet.addAll(getAllSourceGie(gieType));
                    }
                }
            }else {
                giesSet = getAllSourceGie(type);
            }

            // Creates all the links for the gies and fill the csv file
            for (WTPart gie : giesSet) {
                List<ConfigurableReferenceLink> links = new ArrayList<>();
                List<EPMDocument> sies = GieCachedDataHelper.service.getAllSIEsFromGie(gie);
                for (EPMDocument sie : sies) {
                    List<Persistable> objects = GieCachedDataHelper.service.getAllObjectsRelatedToSie(sie);
                    for (Persistable object : objects) {
                        ConfigurableReferenceLink link = GieCachedDataHelper.service.createObjectGieLink(gie, object, sie);
                        if(link != null) {
                            links.add(link);
                        }
                    }
                }
                for(ConfigurableReferenceLink link : links) {
                    Persistable object = link.getRoleBObject();
                    String objectName = "";
                    String objectNumber = "";
                    String gieVersion = "";
                    if(object instanceof WTPartMaster){
                        WTPartMaster partMaster = (WTPartMaster) object;
                        objectName = partMaster.getName();
                        objectNumber = partMaster.getNumber();
                    }else if(object instanceof EPMDocumentMaster){
                        EPMDocumentMaster epmdocMaster = (EPMDocumentMaster) object;
                        objectName = epmdocMaster.getName();
                        objectNumber = epmdocMaster.getNumber();
                    }
                    gieVersion = gie.getVersionInfo().getIdentifier().getValue() + "." + gie.getIterationInfo().getIdentifier().getValue();
                    GIEData gieData = new GIEData(REFERENCE_FACTORY.getReference(gie), gie.getName(), gie.getNumber(), gieVersion, GieCachedDataHelper.service.getGieType(gie));
                    ObjectData objectData = new ObjectData(REFERENCE_FACTORY.getReference(object), objectName, objectNumber, GieCachedDataHelper.service.getObjectType(object).name());
                    // Get count and parent Elements
                    String count = (String) PhenixIBAUtils.getIBAvalue(link, SoftAttributes.COUNT_ATTRIBUTE);
                    String parentElementStr = "";
                    Object parentElement = PhenixIBAUtils.getIBAvalue(link,SoftAttributes.PARENT_ELEMENT_ATTRIBUTE);
                    if(parentElement instanceof String) {
                        parentElementStr = (String) parentElement;
                    }else if(parentElement instanceof Object[]) {
                        Object[] parentElementTab = (Object[]) parentElement;
                        for(Object parentElementObject : parentElementTab) {
                            parentElementStr += ((String) parentElementObject) + ", ";
                        }
                        parentElementStr = parentElementStr.substring(0, parentElementStr.length() - 2);
                    }
                    GieCachedDataInitializationRow csvRow = new GieCachedDataInitializationRow(gieData,objectData,count,parentElementStr);
                    currentFileLines++;
                    csvRow.setProcessed();
                    List<Object> writerValues = writeRowData(csvRow.getLineRow(), outputWriter, outputFile, currentFileLines, outputOption, csvRowsLimit, false);
                    currentFileLines = (Long) writerValues.get(0);
                    outputWriter = (ExportListWriter) writerValues.get(1);
                    outputOption = (String) writerValues.get(2);
                    outputFile = (File) writerValues.get(3);
                }
            }


            LOGGER.info("Writing output file: {}" , outputFile.getAbsolutePath());
            outputWriter.close();

        } catch (Exception e) {
            throw new WTException(e, "An error occured while trying to create output report file.");
        }
    }
