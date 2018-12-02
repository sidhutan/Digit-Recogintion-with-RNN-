class CSEAFMRS_IndexCal:
    
     def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
#        self.CloseComposition.to_csv("CSEAFMRS_CloseComposition.csv")
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
#        self.OpenComposition.to_csv("CSEAFMRS_OpenComposition.csv")
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.DailyPrices["SettlementDate"]=pd.to_datetime(self.DailyPrices["SettlementDate"])
#        self.DailyPrices.to_csv("CSEAFMRS_dailyprices.csv")
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
#        self.IndexSpecificData.to_csv("CSEAFMRS_IndexSpecificData.csv")
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
     
     def settleDates(self,ticker):
         Datum=self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]==ticker)&(self.DailyPrices["PriceDate"]==self.Rundate),"SettlementDate"].values[0]
         return Datum
        
         
        
     def Lg_Returns(self,Price_t,Price_t_1):
         #SPX
         return np.log(Price_t/Price_t_1)
         
     def Delta(self):
         Lev=25
         IL_1=self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"].values[0]
         Run_date_price=self.IndexSpecificData.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"]=="SPTR"),"Price"].values[0]
         Price_array=list(self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="SPTR")&(self.DailyPrices["PriceDate"]!=self.RunDate),["PriceDate","Price"]].sort_values(by="PriceDate").tail(4)["Price"])
         return 2*IL_1*max(-1,Lev*0.2*sum(np.log(Run_date_price)**4)/np.prod(Price_array))
    
     def T_Es(self,ticker):
        #settlementDate=self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]==ticker)&(self.DailyPrices["PriceDate"]==self.RunDate),"SettlementDate"]
        return (self.settleDates(ticker)-self.RunDate)/360

     def Expiry(self,ticker):
         #interpollation, linear
         #SettlementDates Sorted Ascending (smallest to largest)
        settlementDates=list(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"].str.contain("Z=R"))&(self.DailyPrices["PriceDate"]==self.RunDate),["SettlementDate"]].sort_values(by="SettlementDate")["SettlementDate"])
        #GreaterDateIndex
        Index=bisect.bisect(settlementDates,self.settleDates(ticker))
        Near_or_Exact_Date=settlementDates[Index-1]
        Far_Date=settlementDates[Index]
        Near_Date_Price=self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["SettlementDate"]==Near_or_Exact_Date),"Price"]        
        Far_Date_Price=self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["SettlementDate"]==Far_Date),"Price"]
        settlementDates_ordinal=[dt.date.toordinal(i) for i in settlementDates]
        #Finding Slope and Intercept
        Slope_Intercept=np.polyfit(settlementDates_ordinal,[Near_Date_Price,Far_Date_Price],1)
        Slope=Slope_Intercept[0]
        Intercept=Slope_Intercept[1]
        settleDates_number=dt.date.toordinal(self.settleDates(ticker))
        return Slope*settleDates_number+Intercept

        
        
        
        
     def OpenIndexSpecific(self):
        t_indexspecificdata = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,:].copy()
        t_indexspecificdata["PriceDate"] = self.RunDate
        return t_indexspecificdata
        
     @hp.output_file(["VTCloseComposition"])
     def fn_VTCloseComp(self):
#        print(self.NewOpenComposition()) 
        return [self.NewCloseComposition()]
        
     @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
     def fn_VTOpenComp(self):
#         pass
        return [self.NewOpenComposition(), self.OpenIndexSpecific()] 
     
