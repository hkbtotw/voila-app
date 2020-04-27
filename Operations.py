import pandas as pd
from pandas_datareader import data
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import pytz

catDict={ 'currency':['THB=X','EURUSD=X','CNY=X','SGD=X' ,'HKD=X', 'JPY=X' ],
                       'oil':['CL=F'] ,
                       #'stock': ['AOT.BK','INTUCH.BK','^SET.BK']
                       'stock': ['AOT.BK','INTUCH.BK']
                     }

colDict_1={'Date':1,
        'Adj Close':2,
        'UpdateTime':3
}

colDict_2={'Date':1,
        'Volume':6,
        'Adj Close':7,
        'UpdateTime':8
}


class ReadSheet(object):
    def __init__(self):
        self.secret_path_1=r'c:/users/70018928/Quantra_Learning/CheckInOutReminder-e2ff28c53e80.json'
        self.secret_path_2=r'./CheckInOutReminder-e2ff28c53e80.json'
        self.scope= ['https://spreadsheets.google.com/feeds',
                              'https://www.googleapis.com/auth/drive']

    def Authorization(self):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_1, self.scope)
        except:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_2, self.scope)
        client = gspread.authorize(creds) 
        sheet = client.open("DataCollection_1").sheet1
        return sheet

    def Authorization_Currency(self):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_1, self.scope)
        except:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_2, self.scope)
        client = gspread.authorize(creds) 
        sheetCList=[]
        cList=catDict['currency']
        for n in cList:
            sheetCList.append(client.open("DataCollection_Currency").worksheet(n))
        return sheetCList

    def Authorization_Oil(self):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_1, self.scope)
        except:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_2, self.scope)
        client = gspread.authorize(creds) 
        sheetOList=[]
        cList=catDict['oil']
        for n in cList:
            sheetOList.append(client.open("DataCollection_Oil").worksheet(n))
        return sheetOList
    
    def Authorization_Stock(self):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_1, self.scope)
        except:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_2, self.scope)
        client = gspread.authorize(creds) 
        sheetSList=[]
        cList=catDict['stock']
        for n in cList:
            sheetSList.append(client.open("DataCollection_Stock").worksheet(n))
        return sheetSList


    def StrToDate(self,strIn):
        return datetime.strptime(strIn, '%Y-%m-%d')

    def Date2TString(self, dateIn):
        return dateIn.strftime("%Y-%m-%d")

    def GetDateTime(self):
        todayUTC=datetime.today()
        nowUTC=datetime.now()
        # dd/mm/YY H:M:S
        to_zone = pytz.timezone('Asia/Bangkok')

        today=todayUTC.astimezone(to_zone)
        now=nowUTC.astimezone(to_zone)

        todayStr=today.strftime("%Y-%m-%d")
        nowDate = now.strftime("%Y-%m-%d")
        nowTime = now.strftime("%H:%M:%S")

        #print(' today : ',todayStr)
        #print(nowDate, ' ==> ', nowTime)
        return todayStr, nowDate, nowTime
    
    def InsertNewValue_1(self,todayStr, nowDate, nowTime, sheet, dateIn, priceIn):
        lenRecords=len(sheet.get_all_values())
        list_of_hashes=sheet.get_all_records()
        lenHash=len(list_of_hashes)
        print(" len : ",lenRecords)
        lastDate=sheet.cell(lenRecords,1).value
        print(' lastDate : ',lastDate)
        lenDate=len(list_of_hashes[lenHash-1]['Date'])
        if(todayStr == lastDate):
            todayRow=lenRecords
            row_index=todayRow
            col_index=colDict_1['Adj Close']
            message=priceIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_1['UpdateTime']
            message=nowTime
            sheet.update_cell(row_index, col_index,message)
            print('Updated at ', nowTime)
        else:
            todayRow=lenRecords+1
            row_index=todayRow
            col_index=colDict_1['Date']
            message=todayStr
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_1['Adj Close']
            message=priceIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_1['UpdateTime']
            message=nowTime
            sheet.update_cell(row_index, col_index,message)
            print('Updated on ', todayStr, ' :: ', nowTime)

    def InsertNewValue_2(self,todayStr, nowDate, nowTime, sheet, dateIn, priceIn, volumeIn):
        lenRecords=len(sheet.get_all_values())
        list_of_hashes=sheet.get_all_records()
        lenHash=len(list_of_hashes)
        print(" len : ",lenRecords)
        lastDate=sheet.cell(lenRecords,1).value
        print(' lastDate : ',lastDate)
        lenDate=len(list_of_hashes[lenHash-1]['Date'])
        if(todayStr == lastDate):
            todayRow=lenRecords
            row_index=todayRow
            col_index=colDict_2['Adj Close']
            message=priceIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_2['Volume']
            message=volumeIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_2['UpdateTime']
            message=nowTime
            sheet.update_cell(row_index, col_index,message)
            print('Updated at ', nowTime)
        else:
            todayRow=lenRecords+1
            row_index=todayRow
            col_index=colDict_2['Date']
            message=todayStr
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_2['Adj Close']
            message=priceIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_2['Volume']
            message=volumeIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_2['UpdateTime']
            message=nowTime
            sheet.update_cell(row_index, col_index,message)
            print('Updated on ', todayStr, ' :: ', nowTime)


    def LoadSheet_0(self,sheet):
        listSheet = sheet.get_all_values()
        #print(' ==> ',type(listSheet)," :: ",listSheet)
        listHash=sheet.get_all_records()
        #print(' ==> ',type(listHash)," :: ",listHash)

        dfSet=pd.DataFrame()
        lenList=len(listHash)
        colList=listSheet[0]
        print(colList)
        dateList=[]
        priceList=[]
        

        for n in range(0,lenList):
            dateList.append(self.StrToDate(listHash[n][colList[0]]))
            priceList.append(listHash[n][colList[1]])
    
        dfSet=pd.concat([pd.DataFrame(dateList),pd.DataFrame(priceList)],axis=1)
        #print(dfSet.columns)
        dfSet.columns=colList

        return dfSet


    def LoadSheet(self,sheet):
        listSheet = sheet.get_all_values()
        #print(' ==> ',type(listSheet)," :: ",listSheet)
        listHash=sheet.get_all_records()
        #print(' ==> ',type(listHash)," :: ",listHash)

        dfSet=pd.DataFrame()
        lenList=len(listHash)
        colList=listSheet[0]
        #print(colList)
        dateList=[]
        priceList=[]
        updateList=[]
        for n in range(0,lenList):
            dateList.append(self.StrToDate(listHash[n][colList[0]]))
            priceList.append(listHash[n][colList[1]])
            updateList.append(listHash[n][colList[2]])
    
        dfSet=pd.concat([pd.DataFrame(dateList),pd.DataFrame(priceList),pd.DataFrame(updateList)],axis=1)
        dfSet.columns=colList

        dfSet_1=dfSet.dropna().copy().reset_index()
        dfSet_2=dfSet_1.drop(columns=['index'])



        return dfSet_2

    def LoadSheet_2(self,sheet):
        listSheet = sheet.get_all_values()
        #print(' ==> ',type(listSheet)," :: ",listSheet)
        listHash=sheet.get_all_records()
        #print(' ==> ',type(listHash)," :: ",listHash)

        dfSet=pd.DataFrame()
        lenList=len(listHash)
        colList=listSheet[0]
        #print(colList)
        dateList=[]
        priceList=[]
        updateList=[]
        volumeList=[]
        for n in range(0,lenList):
            dateList.append(self.StrToDate(listHash[n][colList[0]]))
            volumeList.append(listHash[n][colList[5]])
            priceList.append(listHash[n][colList[6]])
            updateList.append(listHash[n][colList[7]])
    
        dfSet=pd.concat([pd.DataFrame(dateList),pd.DataFrame(volumeList),pd.DataFrame(priceList),pd.DataFrame(updateList)],axis=1)
        dfSet.columns=['Date','Volume','Adj Close','UpdateTime']

        dfSet_1=dfSet.dropna().copy().reset_index()
        dfSet_2=dfSet_1.drop(columns=['index'])


        return dfSet_2

    def ConvertCurrency_2(self,dfIn,category):
        cList=catDict[category]

        dfTHB=dfIn[0]['Date'].to_frame()
      
        dfTHB=pd.concat([dfTHB,dfIn[0]['Adj Close'].to_frame(),dfIn[1]['Adj Close'].to_frame(),dfIn[2]['Adj Close'].to_frame(),dfIn[3]['Adj Close'].to_frame(),dfIn[4]['Adj Close'].to_frame(),dfIn[5]['Adj Close'].to_frame()], axis=1)
        dfTHB.columns=['Date','THB_USD', 'EUR','CNY','SGD' ,'HKD', 'JPY']
        
        
        dfTHB['THB_EUR']=dfTHB['THB_USD']/dfTHB['EUR']
        dfTHB['THB_CNY']=dfTHB['THB_USD']/dfTHB['CNY']
        dfTHB['THB_SGD']=dfTHB['THB_USD']/dfTHB['SGD']
        dfTHB['THB_HKD']=dfTHB['THB_USD']/dfTHB['HKD']
        dfTHB['THB_JPY']=dfTHB['THB_USD']/dfTHB['JPY']

        dfCon=dfTHB[['Date','THB_USD','THB_EUR','THB_CNY','THB_SGD','THB_HKD','THB_JPY']].copy()
        #print(dfCon.tail(), ' :: ',dfCon.columns)
        return dfCon


class LoadData(object):
    def __init__(self):
        self.QUANDL_API_KEY = 'abe1CkdZn-beCcde_GSt'
        self.start_date= '2015-01-01'
        self.filepath1=r'C:/Users/70018928/Quantra_Learning/data/'
        self.filepath2='data/'


    def LoadYahoo_Data(self,end, category):
        cList=catDict[category]
        dfList=[]
        for n in cList:
            dfList.append(data.get_data_yahoo(n, self.start_date, end))
        return dfList

    def LoadYahoo_Data_NoEnd(self,category):
        cList=catDict[category]
        dfList=[]
        for n in cList:
            dfList.append(data.get_data_yahoo(n, self.start_date))
        return dfList
    
    def WriteData(self,ticker,dfIn):
        try:
            fileout=self.filepath1+ticker+'.csv'
            dfIn.to_csv(fileout)
        except:
            fileout=self.filepath2+ticker+'.csv'
            dfIn.to_csv(fileout)

    def WriteInitialData(self,dfIn,category):
        cList=catDict[category]
        for n in range(0,len(dfIn)):
            filename=self.filepath1+cList[n]+'.csv'
            #print(cList[n])       
            dfIn[n].to_csv(filename)

    def ConvertCurrency(self,dfIn,category):
        cList=catDict[category]
        for n in range(0,len(dfIn)):
            filename=self.filepath1+cList[n]+'.csv'
            #print(cList[n])       
            dfIn[n].to_csv(filename)
        dfTH=dfIn[0]['Adj Close'].to_frame()
      
        dfTH_1=pd.merge(dfTH,dfIn[1]['Adj Close'].to_frame(), how='outer', on='Date')
        dfTH_2=pd.merge(dfTH_1,dfIn[2]['Adj Close'].to_frame(), how='outer', on='Date')
        dfTH_3=pd.merge(dfTH_2,dfIn[3]['Adj Close'].to_frame(), how='outer', on='Date')
        dfTH_4=pd.merge(dfTH_3,dfIn[4]['Adj Close'].to_frame(), how='outer', on='Date')
        dfTH_5=pd.merge(dfTH_4,dfIn[5]['Adj Close'].to_frame(), how='outer', on='Date')
        
        dfTH_5.columns=['THB_USD', 'EUR','CNY','SGD' ,'HKD', 'JPY']

        dfTH_6=dfTH_5.dropna().copy().reset_index()
        dfTHB=dfTH_6.drop(columns=['index'])       


        dfTHB['THB_EUR']=dfTHB['THB_USD']/dfTHB['EUR']
        dfTHB['THB_CNY']=dfTHB['THB_USD']/dfTHB['CNY']
        dfTHB['THB_SGD']=dfTHB['THB_USD']/dfTHB['SGD']
        dfTHB['THB_HKD']=dfTHB['THB_USD']/dfTHB['HKD']
        dfTHB['THB_JPY']=dfTHB['THB_USD']/dfTHB['JPY']

        dfCon=dfTHB[['THB_USD','THB_EUR','THB_CNY','THB_SGD','THB_HKD','THB_JPY']].copy()
        #print(dfCon.tail(), ' :: ',dfCon.columns)
        return dfCon

    

            
         
        
    
    




