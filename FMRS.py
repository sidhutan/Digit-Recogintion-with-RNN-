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
         #SPTR
         Price_t=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]=="SPTR")&(self.DailyPrices.loc["PriceDate"]==self.RunDate),"Price"].values[0]
         Price_t_1=self.DailyPrices.loc[(self.DailyPrices["GenericTicket"]=="SPTR")&(self.DailyPrices.loc["PriceDate"]==self.LastRunDate),"Price"].values[0]
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

     def Borrow(self):
         SPX_divident=self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="SPX")&(self.DailyPrices["PriceDate"]==self.RunDate),"Dividentyield"].values[0]
         T1=self.T_Es("ES1")
         T2=self.T_Es("ES2")
         R1=self.Expiry("ES1")
         R2=self.Expiry("ES2")
         ES1_Price_t=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]=="ES1")&(self.DailyPrices.loc["PriceDate"]==self.RunDate),"Price"].values[0]
         ESES=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]=="S:ESES 1-2")&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
         return (-SPX_divident-(1/(T2-T1))*(R1*T1-R2*T2+(np.log(1+ESES/ES1_Price_t))))
    
     def Fin_rate(self):
         libor_t=self.DailyPrices[(self.DailyPrices["GenericTicker"]=="US0003m")&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]

         if self.Delta()>=0:
             return libor_t-self.Borrow()+(0.001/2)
         else:
             return  libor_t-self.Borrow()-(0.001/2)
     def TA(self):
         #IndexSpecificData
         Delta_t_1=self.IndexSpecificData[(self.IndexSpecificData["PriceDate"]==self.LastRunDate),"IndexNotional"].values[0]
         Finrate_t_1=self.IndexSpecificData[(self.IndexSpecificData["PriceDate"]==self.LastRunDate),"Fee"].values[0]
         days_bet_run_last=(self.RunDate-self.LastRunDate)/np.timedelta64(1, 'D')
         SPTR_t=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]=="SPTR")&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
         SPTR_t_1=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]=="SPTR")&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"].values[0]
         return (Delta_t_1*Finrate_t_1*(days_bet_run_last)/360)+(abs(self.Delta()-(Delta_t_1*SPTR_t/SPTR_t_1-1))*0.0002)
             
     def IndexLevel(self):
         IL_t_1=self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate, "IndexLevel"].values[0]
         Delta_t_1=self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexNotional"].values[0]
         SPTR_t=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]=="SPTR")&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
         SPTR_t_1=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]=="SPTR")&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"].values[0]
         return round(IL_t_1,2)+(Delta_t_1*(SPTR_t/SPTR_t_1 -1 ))-self.TA()

     def OpenIndexSpecific(self):
        t_indexspecificdata = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,:].copy()
        t_indexspecificdata["PriceDate"] = self.RunDate
        t_indexspecificdata["Fee"]=self.Fin_rate()
        t_indexspecificdata["Weights"]=self.Borrow()
        t_indexspecificdata["IndexNotional"]=self.Delta()
        return t_indexspecificdata
    
     def VTOpenComposition(self):
            pass
      def VTCloseComposition(self):
        pass
        
     @hp.output_file(["VTCloseComposition"])
     def fn_VTCloseComp(self):
#        print(self.NewOpenComposition()) 
        return [self.NewCloseComposition()]
        
     @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
     def fn_VTOpenComp(self):
#         pass
        return [self.NewOpenComposition(), self.OpenIndexSpecific()] 
     
