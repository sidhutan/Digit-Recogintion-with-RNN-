class JTRDX2BUCalculation:
    def __init__(self,cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.Returns = cl_FileProcessor.dict_FileData["VTReturnData"]
        self.Returns.to_csv("Return_JTRDX2BU.csv")
        self.Returns["Date"]=pd.to_datetime(self.Returns["Date"])
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.OpenComp.to_csv("OpenComp_JTRDX2BU.csv")
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.CloseComp.to_csv("CloseComp_JTRDX2BU.csv")
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices.to_csv("Daily_Prices_JTRDX2BU.csv")
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.IndexSpecificData.to_csv("IndexSpecificData_JTRDX2BU.csv")
        self.RunDate = cl_FileProcessor.RunDate
        print("RunDate is :",self.RunDate)
        self.LastRunDate = cl_FileProcessor.LastRunDate
        print("LastRunDate:",self.LastRunDate)
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"]).values[0]
#    
##    def Price(self,GenTicker):
###        Price_GenTicker=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]==GenTicker)&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
##        return self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]==GenTicker)&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
###      
##    def Retrn(self,GenTicker):
##        return self.Return.loc[(Return["Ticker"]==GenTicker)&(Return["Date"]==self.RunDate)]
##    def AggregateRetrn(self):
##        Sum_1=map(self.Retrn,["JTRDAFXU","JTRDCFXU","JTRDEFXU","JTRDGFXU","JTRDHFXU","JTRDJFXU","JTRDNFXU"])
##        return sum(list(Sum_1))
    def Ret(self,GenTicker):
        P_t=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]==GenTicker)&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
        P_t_1=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]==GenTicker)&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"].values[0]
        return (P_t/P_t_1-1)*1/7
#    
## def Retrn(GenTicker):
##     return Return.loc[(Return["Ticker"]==GenTicker)&(Return["Date"]==RunDate),"ReturnValue"].values[0]
    def AggregateRetrn(self):
        Sum_1=map(Ret,["JTRDAFXU","JTRDCFXU","JTRDEFXU","JTRDGFXU","JTRDHFXU","JTRDJFXU","JTRDNFXU"])
        print(list(Sum_1))
        print(sum(list(Sum_1)))
        return sum(list(Sum_1))
        
#    np.std(Return.loc[(Return["Ticker"]=="JTRDAFXU")].sort_values(by="Date",ascending=True).tail(72).head(66)["ReturnValue"])
#   ------- Return.loc[(Return["Ticker"]=="JTRDAFXU")&(Return["Date"]==LastRunDate),"ReturnValue"].values[0]
    def stdDev(self,val):
        st_1=[j for i,j in enumerate(Return.sort_values(by="Date",ascending=True).tail(val+6).head(val)["ReturnValue"])]
#        st_1=[j for i,j in enumerate(Return.sort_values(by="Date",ascending=True).tail(77).head(66)["ReturnValue"])]
        return np.std(a,ddof=1)*np.sqrt(252)
    
    def TargetExp(self):
        Min_Exp=0
        Max_Exp=2
        VT=0.04
        return max(Min_Exp,min(Max_Exp,VT/(max(self.stdDev(66),self.stdDev(22)))))
    def RollDate(self):
        return 1 if self.LastRunDate.month!=self.RunDate.month else 0
    
    def IndexNotional(self):
        if self.RollDate==1:
            return self.IndexSpecificData.sort_values(by="PriceDate",ascending=True).tail(4).head(1)["IndexLevel"].values[0]
        else:
            return self.IndexSpecificData.loc[(IndexSpecificData["PriceDate"]==self.LastRunDate),"IndexNotional"].values[0]
#        if self.RollDate==1:
            
            
    def IndexLevel(self):
        IL_1=self.IndexSpecificData.loc[(IndexSpecificData["PriceDate"]==self.LastRunDate),"IndexLevel"].values[0]
        return (IL_1+self.IndexNotional())*self.AggregateRetrn()*self.TargetExp()
        
        #return self.Retrn(JTRDAFXU)+self.Retrn(JTRDCFXU)+self.Retrn(JTRDEFXU)+self.Retrn(JTRDGFXU)+self.Retrn(JTRDHFXU)+self.Retrn(JTRDJFXU)+self.Retrn(JTRDNFXU)
#        R1=self.Return.loc[(Return["Ticker"]=="JTRDAFXU")&(Return["Date"]==self.RunDate)]
#        R1=self.Return.loc[(Return["Ticker"]=="JTRDCFXU")&()]
    def CashUnits(self,IL,df):
        return IL-df[['Units','Price']].product(axis=1).sum()
    
    def Price(self,GenTicker):
        return self.DailyPrices.loc[self.DailyPrices["GenericTicker"]==GenTicker]["Price"].values[0]
    def CloseUnts_ticker(self,GenTicker):
        #GenTicker=JTRDAFXU","JTRDCFXU","JTRDEFXU","JTRDGFXU","JTRDHFXU","JTRDJFXU","JTRDNFXU
        self.TargetExp()*self.IndexNotional()/7*(self.Price(GenTicker))
        
        
    def CloseUnits(self):
        #"JTRDAFXU","JTRDCFXU","JTRDEFXU","JTRDGFXU","JTRDHFXU","JTRDJFXU","JTRDNFXU"
        Unit_JTRDAFXU=self.CloseUnts_ticker("JTRDAFXU")
        Unit_JTRDCFXU=self.CloseUnts_ticker("JTRDCFXU")
        Unit_JTRDEFXU=self.CloseUnts_ticker("JTRDEFXU")
        Unit_JTRDGFXU=self.CloseUnts_ticker("JTRDGFXU")
        Unit_JTRDHFXU=self.CloseUnts_ticker("JTRDHFXU")
        Unit_JTRDJFXU=self.CloseUnts_ticker("JTRDJFXU")
        Unit_JTRDNFXU=self.CloseUnts_ticker("JTRDNFXU")
        return (Unit_JTRDAFXU+Unit_JTRDCFXU+Unit_JTRDEFXU+Unit_JTRDGFXU+Unit_JTRDHFXU+Unit_JTRDJFXU+Unit_JTRDNFXU)
#        
#    def CashUnits(self):
#        return self.TargetExp
#OpenComp.loc[~OpenComp.SpecificTicker.isin(["CONSTANT"])]       
    
    def NewCloseComposition(self):
        t_NewCloseComposition = self.OpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        df = t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price']]
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewCloseComposition
        
    def NewOpenComposition(self):
        t_NewOpenComposition = self.OpenComp.copy()
        t_NewOpenComposition["Date"] = self.RunDate
        
        if self.OpenDate.year != self.RunDate.year :
            t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] =  self.IndexLevel/(3* t_NewOpenComposition[['Price']])
        df = t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price']]
        t_NewOpenComposition.loc[t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewOpenComposition
    
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]
    
    @hp.output_file(["VTOpenComposition"])
    def fn_VTOpenComp(self):
        return [self.NewOpenComposition()]`
