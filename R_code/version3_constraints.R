# load the AxiomaR package

library(rJava)
library(AxiomaR)
library(yaml)

backtest <- function(factor_name, INPUTPATH_alpha,INPUTPATH_bmk, INPUTPATH_universe, INPUTPATH, OUTPUTPATH, LOCAL_RISKMODEL,
                     initialReferenceValue, start_day, end_day,AxiomaDataDir,flag,configfile){
  
  
  local_riskmodel_name = "uqerRiskModel"
  
  RISKMODELS <- "CNAxiomaSH,CNAxiomaSH-S"	# risk models required for the strategy
  RISKMODEL_F <- "CNAxiomaSH"				# risk model for the constraint setting
  RISKMODEL_S <- "CNAxiomaSH-S"			# risk model for the constraint setting
  RISKMODEL   <- "CNAxiomaSH"				# risk model for rebalancing default
  NUMERAIRE <- "CNY";             		# Numeraire for risk model and analytics.  Default is USD
  
  PREEMPTIVE_SELL <- TRUE         #   Pre-Emptive Sell Option - if the asset is not contained in next period's 
  # returns, do not hold the asset in the final holdings
  PRINT_PERIOD_WORKSPACE <- TRUE  # turn this to TRUE if you would like period workspace wsp file
  
  temp1 = substr(INPUTPATH_bmk,1,nchar(INPUTPATH_bmk)-7)
  temp1 = paste0(temp1,'000300/')
  filename = list.files(temp1)
  #filename = list.files("D:/Project/优化�?/axioma_need/Benchmark")
  filename1 <- c()
  for (i in filename) filename1 <- append(filename1,unlist(strsplit(i,split="_"))[2])
  
  LIST_OF_DATES <- c()
  for (i in filename1) LIST_OF_DATES <- append(LIST_OF_DATES,unlist(strsplit(i,split="[.]"))[1])
  
  LIST_OF_DATES = LIST_OF_DATES[LIST_OF_DATES>=start_day & LIST_OF_DATES<=end_day]
  

  # backtest only
  PREEMPTIVE_SELL <- TRUE
  PRINT_PERIOD_WORKSPACE <- TRUE
  
  #
  # Example logic begins here!
  #
  numperiods <- length(LIST_OF_DATES) - 1 # Last period only for return data 
  # purposes.  Rebalancing will not be 
  # run on the last period.
  initialReferenceValue <- 1.0e8
  periodReferenceValue <- initialReferenceValue
  holdings <- c()
  prevAssetIDs <- c()
  
  benchmarkRet_summary <- rep( NA, numperiods )
  returns_summary <- rep( NA, numperiods )
  risk_summary <- rep( NA, numperiods )
  ActiveRisk_summary <- rep( NA, numperiods )
  SpecificRisk_summary <- rep( NA, numperiods )
  Turnover_summary <- rep( NA, numperiods )
  
  cat( "\n" )
  cat("Backtest dates:");
  cat( LIST_OF_DATES );
  cat( paste( "Total number of Periods:", numperiods, "\n") );
  
  for( period in 1 : numperiods ) {
    #
    # Set up the data
    #
    DATE <- LIST_OF_DATES[ period ]
    NEXTDATE <- LIST_OF_DATES[ period + 1 ]
    
    
    wsId <- paste( "PeriodWorkspace_", DATE, sep = "" )
    
    #
    # create workspace
    cat("creating workspace...\n")
    ws <- createWorkspace( id = wsId, desc = "Optional Description", d = DATE, 
                           numeraire = NUMERAIRE )
    
    # Allows you to edit data in workspace once it is created.  This will come 
    # into use when creating composites and adding to the Price data
    setReferenceCountOn( ws, FALSE )
    
    # load period Data
    cat( "\n" )
    cat( paste( "Processing data for Period[", period, "] for date=", DATE, "\n", sep = "" ) );
    # set up the data
    
    cat( "\n" )
    cat( paste( "Processing data for date=", DATE, "\n", sep = "" ) );
    
    benchmarkFile<-read.csv(paste0(INPUTPATH_bmk,"benchmark_",DATE,".csv"), header=FALSE)	## benchmark file
    #alphaFile<-read.csv(paste0(INPUTPATH_alpha,factor_name,"/",factor_name,"_",DATE,".csv"), header=FALSE)	## alpha file
    alphaFile<-read.csv(paste0(INPUTPATH_alpha,factor_name,"/",factor_name,"_raw_CN_",DATE,".csv"), header=FALSE)
    #alphaFile<-read.csv(paste0(INPUTPATH_alpha,factor_name,"/",factor_name,"_neut_CN_",DATE,".csv"), header=FALSE)
    calendarFile<-read.csv(paste0(INPUTPATH,"AXCN-calendar.att"),sep = "|", skip=4) # calendar file (can be found in the risk model folder)
    
    # add variable names to user data
    
    names(alphaFile)=c("Ticker","Alpha")
    names(benchmarkFile)=c("Ticker","Weight")
    
    
    periodData<-merge(benchmarkFile,alphaFile, all=TRUE)
    periodData[is.na(periodData)]<-0  
    
    
    # load data
    
    # load Axioma derby master
    cat("loading master...\n")
    masterdao <- createDAO( workspace = ws, type = "DbMasterProviderDAO", 
                            props = list( dbmeta = "${AxiomaDataDir}DbMeta",
                                          selected_riskmodels = RISKMODELS,
                                          dbmaster = "${AxiomaDataDir}DbMaster",
                                          dbcomposite = "${AxiomaDataDir}DbComposite" ),
                            params = list( AxiomaDataDir = AxiomaDataDir ) )
    
    # load risk model covariance matrix
    cat("loading risk model...\n")
    rmdao <- createDAO( workspace = ws, type = "DbModelProviderDAO", 
                        props = list( dbmeta = "${AxiomaDataDir}DbMeta",
                                      dbmodel = "${AxiomaDataDir}DbModel",
                                      create_returns_classification = "false",
                                      selected_riskmodels = RISKMODELS,
                                      use_selected_riskmodels = "true" ),
                        params = list( AxiomaDataDir = AxiomaDataDir ) )
    
    riskmodel <- Workspace.getRiskModel(ws, RISKMODEL)
    riskmodel_s <- Workspace.getRiskModel(ws, RISKMODEL_S)
    riskfactornames <- getFactorNames(riskmodel)
    
    # load Axioma supplied attributes (e.g. Price, Marcket Cap, ADV etc)
    cat("loading axioma attributes...\n")
    assetdao <- createDAO( workspace = ws, type = "DbAssetProviderDAO",
                           props = list( dbmeta = "${AxiomaDataDir}/DbMeta", 
                                         dbasset = "${AxiomaDataDir}/DbAsset",
                                         dbcomposite = "${AxiomaDataDir}/DbComposite"),
                           params = list( AxiomaDataDir = AxiomaDataDir))
    
    cat("loading axioma asset returns...\n")
    rtndao <- createDAO( workspace = ws, type = "DbAssetCumRtnDAO",
                         props = list( period_date = DATE,
                                       selected_riskmodels = RISKMODEL,
                                       dbmeta = "${AxiomaDataDir}/DbMeta", 
                                       dbasset = "${AxiomaDataDir}/DbAsset",
                                       dbcomposite = "${AxiomaDataDir}/DbComposite",
                                       next_period_date_format = "yyyyMMdd",
                                       next_period_date = NEXTDATE),
                         params = list( AxiomaDataDir = AxiomaDataDir))
    
    
    # load Axioma supplied GICS
    cat("loading GICS...\n")
    gicsdao <- createDAO( workspace = ws, type = "DbClassificationProviderDAO",
                          props = list( dbclassification = "${AxiomaDataDir}/DbClassification"),
                          params = list( AxiomaDataDir = AxiomaDataDir ) )  				   
    
    # load assets using ticker map
    cat("loading list of assets...\n")
    ws_notify <- Workspace.setAutoNotifyDataProviders(ws,FALSE)
    tickermap <- Workspace.getAssetMap(ws, "Ticker Map")
    
    if (flag == 1){
      ## read local risk model data
      testrisk<-read.csv(paste0(LOCAL_RISKMODEL,"Covariance/Covariance_",DATE,".csv"), header=TRUE,row.names=1)
      testrisk2<-read.csv(paste0(LOCAL_RISKMODEL,"Exposure/Exposure_",DATE,".csv"), header=TRUE,row.names=1,skip=1)
      
      covariances = as.matrix(testrisk)
      specificRisks = testrisk2$X.1
      factorExposures <- subset(testrisk2, select = -X.1 )
      factorNames = colnames(factorExposures)
      assetNames = rownames(factorExposures)
      t1 = Sys.time()
      axiomaid2<-c()
      dataExposures <- c()
      dataspecificRisk <- c()
      for( i in 1 : length(assetNames) ) {
        #   a <- Workspace.findAsset(instance = ws,assetMapping = toString(periodData[i,1]), assetMap = tickermap)
        a <- Workspace.findAsset(instance = ws,assetMapping = toString(assetNames[i]), assetMap = tickermap)
        # rebuild list of covered assets
        if(!is.null(a)){
          axiomaid2 <- append(axiomaid2, getIdentity(a))
          dataExposures <- rbind(dataExposures, as.matrix(factorExposures[i,]))
          dataspecificRisk <- append(dataspecificRisk, specificRisks[i])
        }
      }
      
      ws_notify <- Workspace.notifyDataProviders(ws);
      ws_notify <- Workspace.setAutoNotifyDataProviders(ws,TRUE)
      
      factorExposures <- as.data.frame(dataExposures)
      factorExposures <- as.matrix(factorExposures)
      # factorExposures <- t(factorExposures)
      
      local_riskmodel <- createFactorRiskModel( ws = ws, id = local_riskmodel_name,
                                                symbols = axiomaid2, fz = factorNames, sr = dataspecificRisk,
                                                factorExp = factorExposures, covariances = covariances )
      t2 = Sys.time()
      print(t2 - t1)
      cat("\n")
    }else{
      axiomaid<-c()

      for( i in 1 : nrow(periodData) ) {
        a <- Workspace.findAsset(instance = ws,assetMapping = toString(periodData[i,1]), assetMap = tickermap)
        # rebuild list of covered assets
        if(!is.null(a)){
          axiomaid <- append(axiomaid, getIdentity(a))

        }    
      }
      ws_notify <- Workspace.notifyDataProviders(ws);
      ws_notify <- Workspace.setAutoNotifyDataProviders(ws,TRUE)
    }
    
    ## create a ticker<->axid map
    tickerMapDF<-Workspace.getAssetMap(instance = ws,id = "Ticker Map",valueExtract = TRUE)
    names(tickerMapDF)<-"Ticker"
    tickerMapDF$axid<-row.names(tickerMapDF)
    names(tickerMapDF)=c("Ticker","axId")
    
    periodDataWithaxid<-merge(periodData,tickerMapDF, by="Ticker")
    axiomaid <- periodDataWithaxid$axId
    # extract non-zero benchmark assets
    Benchmark <- as.data.frame(list(periodDataWithaxid$axId, periodDataWithaxid$Weight))
    Benchmark <- Benchmark[Benchmark[, 2] != 0,]
    axID <- as.vector(Benchmark[,1])
    benchmarkWeight <- as.vector(Benchmark[,2])
    
    
    # create attributes
    cat("loading user supplied data...\n")
    
    
    # set alpha group from user data
    alphaGroup <- createSimpleGroup(ws=ws,id="Alpha",desc="",date = DATE,unit=.Unit.NUMBER,
                                    symbols=periodDataWithaxid$axId, values=periodDataWithaxid$Alpha)  # alpha group  
    
    # set benchmark group from user data
    # benchmarkWeight <- benchmarkWeight/sum(benchmarkWeight)
    benchmarkGroup <- createBenchmark( ws = ws, id = "CSI 300", unit = .Unit.PERCENT, 
                                       symbols = axID, values = benchmarkWeight) 
    
    benchmarkGroup_holding <- createSimpleGroup( ws = ws, id = "HS300", unit = .Unit.NUMBER,
                                       symbols = axID, values = as.numeric(rep(1,length(axID))))

    Benchmark <- Workspace.getGroup( instance = ws, id = "CSI 300", valueExtract = TRUE)
    
    # define local universe
    universeFile<-read.csv(paste0(INPUTPATH_universe,"universe_",DATE,".csv"), header=FALSE)
    names(universeFile)=c("Ticker","w")
    universeWithaxid<-merge(universeFile,tickerMapDF, by="Ticker")

    universeGroup <- createSimpleGroup(ws=ws,id="local_universe",unit=.Unit.NUMBER,
                                       symbols=universeWithaxid$axId, values=as.numeric(universeWithaxid$w))
    # universeSet <- createAssetSet(ws, id = universeid)
    cat("#################################################\n")
    # set Default price group to axioma supplied prices
    Workspace.setDefaultPriceGroup( ws, g = Workspace.getGroup(ws, id = "Price"))
    prices <- Workspace.getGroup( ws, id = "Price", valueExtract = TRUE )
    names( prices ) <- c( "axId", "price" )
    
    # axioma supplied asset returns attribute  
    periodReturns <- Workspace.getGroup( ws, id = "Period Return", valueExtract = TRUE )
    
    
    # Buy cost in unit Currency ($ per $ traded)
    buyCost  <- 0.001
    # Sell cost in unit Currency ($ per $ traded)
    sellCost <- 0.002
    tcm <- createTransactionCostModel( ws = ws, id = "TCostModel",
                                       symbols = axID, buyCost = rep( buyCost, length(axID) ),
                                       sellCost = rep( sellCost, length(axID) ), unit = .Unit.CURRENCY )
    
    
    if (flag == 1){
      risk_model_use = local_riskmodel
      risk_model_name = local_riskmodel_name
      model_size_name = 'SIZE'
    } else{
      risk_model_use = riskmodel
      risk_model_name = RISKMODEL
      model_size_name = 'Size'
    }
    cat( "\n" )
    cat( paste( "Using risk model:    ", risk_model_name, "\n", sep = "" ) );
    
    
    # build portfolio optimization Strategy
    # create strategy object to tie objective terms and constraints together
    strategy <- createStrategy( ws = ws, id = "HS_300_Enhanced", desc="", d=getDate(ws) )
    Strategy.setAllowShorting( strategy, FALSE )
    Strategy.setAllowCrossover( strategy, TRUE)
    Strategy.setAllowGrandfathering( strategy, TRUE)
    Strategy.setIgnoreCompliance( strategy, FALSE )	
    Strategy.setIgnoreRoundLots( strategy, FALSE )
    # get elements to be used in strategy
    masterSet <- Workspace.getMasterSet( ws )
    localset <- Workspace.getLocalUniverseSet( ws )
    industryGroup <- Workspace.getMetagroup(ws, id = paste( RISKMODEL_F, "Industry Groups", sep = "." ), valueExtract = FALSE)
    
    
    #### set intersect alpha and local universe 
    intersectID = intersect(universeWithaxid$axId,periodDataWithaxid$axId)
    intersectGroup <- createSimpleGroup(ws=ws,id="intersect_universe",unit=.Unit.NUMBER,
                                        symbols=intersectID, values=as.numeric(rep(1,length(intersectID))))
    kappa1 = sqrt(qchisq(0.95,length(intersectID)))
    print(kappa1)
    # set local universe to stocks in benchmark
    lu <- Strategy.getLocalUniverse(strategy)
    
    # no wrapper methods for local universe setup. calling java methods directly to reset and add contents
    lu$reset()
    lu$includeGroup(intersectGroup)

    
    #
    # Create Objective(s)
    # 1. Robust_Objective
    robust_Objective <- createObjective(strategy, id="Robust_Objective", desc="")
    Objective.setTarget( robust_Objective, .Objective.Target.MAXIMIZE )
    
    Objective_set = masterSet#localset 
    # Robust Objective Term
    # i. user_Alpha_Robust
    Alpha_Robust <- createRobustTerm(s = strategy, id = "Alpha_Robust", desc = "")
    setAlphaGroup( instance = Alpha_Robust, g = alphaGroup )
    setBaseAssetSet(instance = Alpha_Robust, assets = Objective_set)
    alphaUncertaintyModel <- createAlphaUncertaintyModel(ws, .Unit.NUMBER)
    setIdentity(instance = alphaUncertaintyModel, id = "oneThirdStdevAlpha")
    AlphaUncertaintyModel.setDefaultUncertainty( alphaUncertaintyModel, 0.1 ) 
    RobustTerm.setAlphaUncertaintyModel(instance = Alpha_Robust, aumodel = alphaUncertaintyModel)
    RobustTerm.setKappa(instance = Alpha_Robust, kappa = kappa1)
    Objective.addObjectiveTerm( instance = robust_Objective, term = Alpha_Robust, weight = 1.0 )
    
    activeVarianceTerm <- createVarianceTerm( s = strategy, id = "Active_Variance", desc = "" )
    setBenchmark( instance = activeVarianceTerm, benchmark = benchmarkGroup)
    setRiskModel( instance = activeVarianceTerm, riskmodel = risk_model_use )
    setGroup( instance = activeVarianceTerm, assets = Objective_set )
    Objective.addObjectiveTerm( instance = robust_Objective, term = activeVarianceTerm, weight = -0.8 )
    
    ##  之前一定要有createTransactionCostModel
    tcostTerm <- createTransactionCostTerm( s = strategy, id = "tcostObj", desc = "" )
    setBaseAssetSet( instance = tcostTerm, assets = Objective_set )
    Objective.addObjectiveTerm( instance = robust_Objective, term = tcostTerm, weight = -0.2 )
    
    # #2. Traditional MVO objective
    # 
    # MVO_Objective <- createObjective(strategy, id="MVO_Objective", desc="")
    # Objective.setTarget( MVO_Objective, .Objective.Target.MAXIMIZE )
    # 
    # # Define and add objective terms
    # # i. User Alpha
    # expectedReturnTerm <- createExpectedReturnTerm( s = strategy, id = "Alpha", desc = "" )
    # setAlphaGroup( instance = expectedReturnTerm, g = alphaGroup )
    # Objective.addObjectiveTerm( instance = MVO_Objective, term = expectedReturnTerm, weight = 1.0 )
    # 
    # # v. Min Active Variance Objective Term
    # activeVarianceTerm <- createVarianceTerm( s = strategy, id = "Active_Variance", desc = "" )
    # setBenchmark( instance = activeVarianceTerm, benchmark = benchmarkGroup)
    # setRiskModel( instance = activeVarianceTerm, riskmodel = riskmodel_s )
    # setGroup( instance = activeVarianceTerm, assets = masterSet )
    # # v. Min Active Variance Objective Term
    # Objective.addObjectiveTerm( instance = MVO_Objective, term = activeVarianceTerm, weight = -1.0 )
    # 
    
    #Set the active objetive for this Strategy, robustObjective or MVO_Objective
    activeObjective <- robust_Objective
    #activeObjective <- MVO_Objective
    Strategy.setActiveObjective(strategy, activeObjective)
    
    # 
    # Create constraints
    #
    # 1. Create Scope Aggregate Limit Holdings constraints, which equals net holdings
    if(configfile$budget$use){
      minNetValue <- configfile$budget$Min
      maxNetValue <- configfile$budget$Max
      createConstraint( strategy = strategy, type = "LimitHoldingConstraint", 
                        id = "Budget_Invest_All_Cash", scope = .Constraint.Scope.AGGREGATE, 
                        unit = .Unit.PERCENT, props = list( min = minNetValue, max = maxNetValue ),
                        selection = masterSet )
    }

    if(configfile$Limit_industry$use){
      minSector <- configfile$Limit_industry$Min
      maxSector <-  configfile$Limit_industry$Max
      createConstraint( strategy = strategy, type = "LimitExposureConstraint",
                        id = "Limit_Industry", scope = .Constraint.Scope.SELECTION,
                        unit = .Unit.PERCENT, props = list( min = minSector, max = maxSector, 
                                                            benchmark = benchmarkGroup ),
                        selection = industryGroup )
    }

    if(configfile$Limit_size$use){
      minSector <- configfile$Limit_size$Min
      maxSector <-  configfile$Limit_size$Max
      sizeGroup = Workspace.getGroup(ws, id = paste( risk_model_name, model_size_name, sep = "." ), valueExtract = FALSE)
      createConstraint( strategy = strategy, type = "LimitExposureConstraint",
                        id = "Limit_Size", scope = .Constraint.Scope.AGGREGATE,
                        unit = .Unit.NUMBER, props = list( min = minSector, max = maxSector,
                                                           benchmark = benchmarkGroup ),
                        selection = sizeGroup )
    }

    if(configfile$TE_limit$use){
      maxTotalRiskInPercent <- configfile$TE_limit$Max;
      createConstraint( strategy = strategy, type = "LimitTotalRiskConstraint", 
                        id = "Tracking_Error_Limit_Fundamental", scope = .Constraint.Scope.AGGREGATE, 
                        unit = .Unit.PERCENT, props = list( max = maxTotalRiskInPercent, 
                                                            riskmodel = risk_model_use,
                                                            benchmark = benchmarkGroup), 
                        selection = masterSet)
    }
   

    if(configfile$stock_weight_limit$use){
      maxValue <- configfile$stock_weight_limit$Max
      createConstraint( strategy = strategy, type = "LimitAbsoluteExposureConstraint",
                        id = "Limit_Asset_Absolute_Exposure", scope = .Constraint.Scope.ASSET,
                        unit = .Unit.PERCENT, props = list( max = maxValue),
                        selection = masterSet )
    }
   
    if(configfile$Benchmark_holding$use){
      minholding <- configfile$Benchmark_holding$Min
      createConstraint( strategy = strategy, type = "LimitHoldingConstraint",
                        id = "Limit_Benchmark_holding", scope = .Constraint.Scope.AGGREGATE,
                        unit = .Unit.PERCENT, props = list( min = minholding),
                        selection = benchmarkGroup_holding )
    }

    
    # # 2. Create Classification level Limit Holdings Constraint
    # minSector <- -2
    # maxSector <-  2
    # createConstraint( strategy = strategy, type = "LimitHoldingConstraint",
    #                   id = "Industry_Active_Holding_Limit", scope = .Constraint.Scope.MEMBER,
    #                   unit = .Unit.PERCENT, props = list( min = minSector, max = maxSector, 
    #                                                       benchmark = benchmarkGroup ),
    #                   selection = industryGroup, priority = 1 )
    
    
    # The hierarchy would be automatically enabled when using constraints in ConstraintUtils
    # setEnabled( Strategy.getConstraintHierarchy( strategy ), TRUE )
    
    if( PREEMPTIVE_SELL ) {
      # Pre-Emptive Sell Option - if the asset is not contained in next period's 
      # returns, do not hold the asset in the final holdings
      periodReturnAssets <- periodReturns$names
      
      # Set of assets not in next period's return
      noReturnAssets <- axiomaid[ !axiomaid %in% periodReturnAssets ]  
      
      if( length( noReturnAssets ) > 0 ) {
        createSimpleGroup( ws = ws, id = "NoReturns", unit = .Unit.PERCENT,
                           assetVals = data.frame( names = noReturnAssets, 
                                                   values = rep( 0, length( noReturnAssets ) ) ) )
        createConstraint( strategy = strategy, type = "LimitHoldingConstraint", 
                          id = "xPreEmptiveSell", scope = .Constraint.Scope.ASSET, unit = .Unit.PERCENT, 
                          selection = list( list( selection = masterSet, props = list( 
                            min_values_group = "NoReturns", 
                            max_values_group = "NoReturns" ) ) ) )
      }
    }
    
    names( prices ) <- c( "axId", "price" )
    initialAccount <- NULL
    
    if( period > 1 ) {
      initHoldings <- data.frame( axId = prevAssetIDs, holdings = holdings )
      initHoldings <- merge( initHoldings, prices, by = "axId", all = TRUE )
      initHoldings$holdings <- initHoldings$holdings / initHoldings$price
      holdings <- initHoldings$holdings[ !is.na( initHoldings$holdings ) ]
      ids <- as.character( initHoldings$axId[ which( !is.na( initHoldings$holdings ) ) ] )
      
      # Exception handling
      # prices should be always availabe
      # if not, we can check "ids" against preAssetIDs
      initialAccount <- createAccount( ws = ws, id = "Account", 
                                       assetNames = ids, assetValues = holdings )
    } else { # the 1st period
      # Create an account with no holdings
      # i.e. initial account starts from cash
      initialAccount <- createAccount( ws = ws, "Account", desc = "", 
                                       d = getDate( ws ), h = data.frame( names = c(), values = c() ) )
    }
    
    setReferenceSize( initialAccount, periodReferenceValue )
    setRoundLotSize( initialAccount, 100)
    rebal <- createRebalancing( strategy = strategy, account = initialAccount, 
                                referenceSize = periodReferenceValue )
    setTransactionCostModel( rebal, tcm )
    setIdentity(instance = rebal, id = paste("Period_Rebalancing_", DATE, sep =""))
    status <- Rebalancing.solve( rebal )
    
    if( status == .RebalancingBase.Status.NoSolutionFound || status == .RebalancingBase.Status.BaseProblemHasNoSolution) {
      cat( paste( "ERROR in solving rebalancing on date=", DATE, sep = "" ) )
      WorkspaceWriter.write( ws = ws, filename = paste( "NoSolution", DATE, ".xml", sep = "" ), 
                             withReference = TRUE )
      return
    } 
    else {
      cat( paste( "Solution for period ", period, ":\n", sep = "" ) )
      solution <- getSolution( rebal )
      finalShares <- getFinalHoldings( solution, valueExtract = TRUE )
      finalShares2 <- getFinalHoldings( solution, valueExtract = TRUE )
      initalShares <- getInitialHoldings( solution, valueExtract = TRUE)
      names( finalShares ) <- c( "axId", "shares" )
      
      finalHoldings <- merge( finalShares, prices, by = "axId", all = TRUE )
      
      names( periodReturns ) <- c( "axId", "return" )
      finalHoldings <- merge( finalHoldings, periodReturns, by = "axId", all = TRUE )
      
      ### 新添加的
      names( Benchmark ) <- c( "axId", "values" )
      Benchmark$values <- Benchmark$values/sum(Benchmark$values)
      finalbenchmark <- merge( Benchmark, periodReturns, by = "axId", all = TRUE ) 
      Retbenchmark = sum(finalbenchmark$values * finalbenchmark$return, na.rm = TRUE)
      benchmarkRet_summary[ period ] <- Retbenchmark
      
      # If the return is NA, reset to 0
      finalHoldings$return[ is.na( finalHoldings$return ) ] <- 0
      finalHoldings$shares[ is.na( finalHoldings$shares ) ] <- 0
      
      
      if(!file.exists(paste0(OUTPUTPATH, factor_name,"_finalhodings"))){
        dir.create(paste0(OUTPUTPATH, factor_name,"_finalhodings"))
      }
      fhodingname <- paste( OUTPUTPATH, factor_name,"_finalhodings","/","finalhodings_", DATE, ".csv", sep = "" )
      
      finalHoldings = merge( finalHoldings, tickerMapDF, by = "axId", all = TRUE )
      write.csv(finalHoldings, file = fhodingname,row.names = FALSE,quote = FALSE)
      
      
      finalHoldings$values <- finalHoldings$shares * finalHoldings$price * ( 1 + finalHoldings$return )
      
      # Get the asset ids and its holdings
      holdings <- finalHoldings$values[ !is.na( finalHoldings$values ) ]
      prevAssetIDs <- as.character( finalHoldings$axId[ which( !is.na( finalHoldings$values ) ) ] )
      
      # Collect portfolio statistics over periods;
      analyzer <- createAnalytics( Workspace.getDefaultPriceGroup( ws ) )
      pReturn <- sum( finalHoldings$shares * finalHoldings$price * finalHoldings$return )
      print( paste( "Period Return =", pReturn ) )
      returns_summary[ period ] <- pReturn / periodReferenceValue
      risk_summary[ period ] <- Analytics.computeTotalRisk( instance = analyzer, 
                                                            riskmodel = risk_model_use, holdings = data.frame( names = finalHoldings$axId, 
                                                                                                               values = finalHoldings$shares ) ) / periodReferenceValue
      ActiveRisk_summary[ period ] <- Analytics.computeActiveTotalRisk( instance = analyzer, 
                                                                        riskmodel = risk_model_use, holdings = data.frame( names = finalHoldings$axId,values = finalHoldings$shares ),
                                                                        benchmark = benchmarkGroup, 
                                                                        referenceValue = periodReferenceValue) / periodReferenceValue
      
      SpecificRisk_summary[ period ] <- Analytics.computeActiveSpecificRisk( instance = analyzer, 
                                                                             factorRiskModel = risk_model_use, holdings = data.frame( names = finalHoldings$axId, 
                                                                                                                                      values = finalHoldings$shares ), benchmark = benchmarkGroup,
                                                                             referenceValue = periodReferenceValue) / periodReferenceValue
      
      Turnover_summary[period] <- Analytics.computeTurnover(instance = analyzer, from = initalShares, to = finalShares2) / periodReferenceValue
      
      periodReferenceValue = periodReferenceValue + pReturn;
    }
    
    # Write Workspace XML
    if( PRINT_PERIOD_WORKSPACE ) {
      #    OUTPUTPATH1 = substring(OUTPUTPATH,1,nchar(OUTPUTPATH)-1)
      if(!file.exists(paste0(OUTPUTPATH, factor_name))){
        dir.create(paste0(OUTPUTPATH, factor_name))
      }
      workspacename <- paste( OUTPUTPATH, factor_name,"/","PeriodWorkspace_", DATE, ".wsp", sep = "" )
      cat( paste( "Writing Workspace at period[", period, 
                  "] to [", workspacename, "]\n", sep = "" ) )
      WorkspaceWriter.write( ws = ws, filename = workspacename, withReference = TRUE )
    }
    
    if( period < numperiods ) {
      destroy(ws)
    }
  }
  
  cat( paste( "\nPeriod Summary:\n" ) )
  cat( sprintf( "%10s%10s%10s\n", "Date", "Return", "Risk" ) )
  
  for( i in 1 : length( returns_summary ) ) {
    cat( sprintf( "%10s%10.4f%10.4f\n", LIST_OF_DATES[ i ], 
                  returns_summary[ i ], risk_summary[ i ] ) )
  }
  
  ## Export the backtest period summary 
  
  Period<-LIST_OF_DATES[1:(length(LIST_OF_DATES)-1)]
  SummaryOutput<-data.frame(Period,returns_summary,benchmarkRet_summary,risk_summary,ActiveRisk_summary,SpecificRisk_summary,Turnover_summary)
  names(SummaryOutput)<-c("Period","Period Total Return","Benchmark Return","Total Risk","Active Risk","Active Specific Risk","Turnover")
  SummaryOutput[["Active Return"]] = SummaryOutput[["Period Total Return"]] - SummaryOutput[["Benchmark Return"]]
  write.csv(SummaryOutput, file = paste0(OUTPUTPATH,factor_name,"Period_Summary.csv"),row.names = FALSE,quote = FALSE)
  
  cat("releasing license...\n")
  AxiomaPortfolioAPI.tearDown()
  return
}


config_path = "E:/QUANT/R_code/config.yaml" #'D:/Project/优化�?/axioma_need/config.yaml' 
configfile <- yaml.load_file(config_path)

# define variables
# inputs

INPUTPATH_bmk1<-configfile$INPUTPATH_bmk   	## input directory
#INPUTPATH_bmk<-"D:/Project/优化�?/axioma_need/Benchmark/" 
INPUTPATH_alpha1<-configfile$INPUTPATH_alpha    	## input directory
#INPUTPATH_alpha<-"D:/Project/优化�?/axioma_need/alpha/"
INPUTPATH_universe1<-configfile$INPUTPATH_universe

INPUTPATH1<-configfile$INPUTPATH    	## input directory
OUTPUTPATH1<-configfile$OUTPUTPATH    	## output directory

LOCAL_RISKMODEL1 <- configfile$LOCAL_RISKMODEL
flag <- as.integer(configfile$flag)

# parameters
initialReferenceValue1 <- as.integer(configfile$initialReferenceValue)     	# initial reference value

start_day1 = as.character(configfile$start_day)
end_day1 = as.character(configfile$end_day)
factor_name = configfile$factor_name
AxiomaDataDir1 = configfile$AxiomaDataDir # the path to the risk model directory

factor_list = unlist(strsplit(factor_name,split=","))

OUTPUTPATH11 = substr(OUTPUTPATH1,1,nchar(OUTPUTPATH1)-1)
if(!file.exists(OUTPUTPATH11)){
  dir.create(OUTPUTPATH11)
}

for (n in factor_list) {
  factor_name1 = n
  backtest(factor_name1, INPUTPATH_alpha1,INPUTPATH_bmk1, INPUTPATH_universe1, INPUTPATH1, OUTPUTPATH1,LOCAL_RISKMODEL1,
           initialReferenceValue1, start_day1, end_day1, AxiomaDataDir1, flag,configfile)
  
}


file_name = "E:/QUANT/Axioma_backtest/"
python.load(file_name) 

