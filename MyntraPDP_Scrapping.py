import scrapy
import pandas as pd
import datetime
import re
import json
from cleantext import clean
from urllib.parse import urlparse
from scrapy.crawler import CrawlerProcess
from fake_useragent import UserAgent
import random
import time
from twisted.internet import reactor, defer
from scrapy.utils.log import configure_logging
from scrapy.crawler import CrawlerRunner
from google.cloud import storage
import json
import os
import sys


#Directory of this python code
BASE_DIR = os.path.dirname(os.path.abspath('__file__'))


#Directories of the input,Exception and output folder through OS
input_dir = os.path.join(BASE_DIR + "\\Input")
error_dir = os.path.join(BASE_DIR + "\\Exception")
output_dir = os.path.join(BASE_DIR + "\\OutputData")
no_filtered_dir = os.path.join(BASE_DIR + '\\No_FilteredData')


#Global Variable to strore the data 
product_data_list =[]
error_url =[]
exception_id = []
Exception_URL = []
trying_url = []
all_output_lis = []
Error_Ids = []
Data_without_filtered = []
No_json_loads = []

#This parse function is called for every each product and extracts the data which we specified.
def parse(response):
    #This try will first clear the error folder data then the function continues
    try:
        if(len(pd.read_excel(r"{}\\Myntra_ErrorIds.xlsx".format(error_dir)))>0):
            pd.DataFrame({'Exception_Id': []}).to_excel(r"{}\\Myntra_ErrorIds.xlsx".format(error_dir))
    except:pass
    #Reading the global variables in function
    global Error_Ids, Exception_URL, Data_without_filtered, all_output_lis, No_json_loads
    #Reading the product ID through the def_request function
    asin = response.meta['ASIN']

    #Extracting the necessary data from the backend
    parent_page = str(response.body)
    main_page = str(response.body)

    if(response.status == 403 or response.status == 503):
        print(int(re.findall(r'\d{5,9}', response.url)[0]))
        Error_Ids.append(int(re.findall(r'\d{5,9}', response.url)[0]))
    #As we are extracting data from backend so we need to decalred some start and end keywords where the data lies between
    #Below is the code which will consider the data laying point's
    try:
        start_string = '{"pdpData'
        json_start = main_page.find(start_string)
        main_page = main_page[json_start:]
        ending = main_page.find("</script>")
        json_string = main_page[0:ending].replace( "\\\\", "")
        json_data_str=clean(json_string, no_emoji=True,lower=False)
        
        start_offers_str = main_page.find('{"android"')
        start_division = parent_page.find('"itemListElement":')+len('"itemListElement":')
        json_division_str = parent_page[start_division:]
        end_division = json_division_str.find("]")+1
        json_division_str =  json_division_str[:end_division]
        json_division_str = clean(json_division_str,no_emoji=True,lower=False)
    #If we don't get the backend data then the ID will move to error variable
    except:
        No_json_loads.append(int(re.findall(r'\d{5,9}', response.url)[0]))
        Error_Ids.append(int(re.findall(r'\d{5,9}', response.url)[0]))
    #This is the another backend data to extract some columns from the page source
    try:
        json_data = json.loads(json_data_str)#It will convert the string into json data, so that we can aceess items easily
        json_data = json_data['pdpData']#starting point of json string
        try:json_division = json.loads(json_division_str)
        except: pass
        #Below mentioned strings are the columns names we are setting here, if we want to add new column we can specify below
        product_col = ['Brand', 'Title', 'MRP', 'Selling Price', 'Discount','Product Rating', 'Category','Sub-Category',
                        'Division','Product Type', 'No of Sizes',
                        'Total Sizes', 'No of Available Sizes', 'Available Sizes', 'No of Non-Available Sizes', 'Non-Available Sizes', 
                        'Product Details', 'Fabric','No of Colors', 'No. of Offers','Color', 'Offers', 'No of Images','Image URL','In Stock','Count of Ratings',
                        'Count of Reviews','COD','Seller URL']
        temp_dict = {}#Storing data temporary
        temp_dict.update({"Date": datetime.date.today().strftime("%Y-%m-%d")})
        today = datetime.date.today()

        #These are constant column calculated for the output columns
        temp_dict.update({"Week": today.isocalendar()[1]})
        temp_dict.update({"Marketplace": urlparse(response.url).netloc.replace('www.','').replace('.com','').title()})
        temp_dict.update({"Product URL": response.url})
        temp_dict.update({"Product ID": json_data['id']})
        
        #The below loop will loop into all the column names and scrape data for the each column and assign it to the column name as variable
        for key in product_col:
            try:
                #Data will extract from the json data converted above if data is not found then blank data will be assigned for the particular column each ProductID
                if(key=='Brand'): val = json_data['brand']['name'].title()
                elif(key=='Title'): val = json_data['name'].replace(json_data['brand']['name']+' ',"").title()
                elif(key=='MRP'): val = json_data['mrp']
                elif(key=='Selling Price'): val = json_data['price']['discounted']
                elif(key=='Discount'):
                    if(json_data['flags']['outOfStock'] == True and json_data['mrp'] == json_data['price']['discounted']):
                        val = ''
                    else:
                        val = json_data['discounts'][0]['discountPercent']
                elif(key=='Product Rating'): val = round(json_data['ratings']['averageRating'],1)
                elif(key=='Category'): val = json_division[0]['item']['name']
                elif(key=='Sub-Category'): val = json_division[2]['item']['name']
                elif(key=='Division'): val = json_division[1]['item']['name']
                elif(key == 'Product Type'):
                    prd_type = json_division[3]['item']['@id']
                    spt_prd_type = prd_type.split("/")
                    val_prd_type = spt_prd_type[3]
                    val = val_prd_type.replace("-"," ")
                    # print("===========", val)
                elif(key=='No of Sizes'): val = len([size['label'] for size in json_data['sizes']])
                elif(key=='Total Sizes'):
                     sizes = [size['label'] for size in json_data['sizes']]
                     val = sizes if len(sizes)>=1 else ''
                elif(key=='No of Available Sizes'): val = len([size['label'] for size in json_data['sizes'] if size['available']==True])
                elif(key=='Available Sizes'):
                     av_sizes = [size['label'] for size in json_data['sizes'] if size['available']==True]
                     val = av_sizes if len(av_sizes)>=1 else ''
                elif(key=='No of Non-Available Sizes'): val = len([size['label'] for size in json_data['sizes'] if size['available']==False])
                elif(key=='Non-Available Sizes'):
                     nonAv_sizes = [size['label'] for size in json_data['sizes'] if size['available']==False]   
                     val = nonAv_sizes if len(nonAv_sizes)>=1 else '' 
                elif(key=='Product Details'): val = json_data['productDetails'][0]['description']
                elif(key=='Fabric'): val = json_data['articleAttributes']['Fabric']
                elif(key=='No. of Offers'): val = len([offer['title'] for offer in json_data['offers']])
                elif(key=='Offers'):
                    offers = []
                    Offer_Len = json_data['offers']
                    for each_offer in Offer_Len:
                        offers.append(str(each_offer['title']))
                        offers.append(str(each_offer['description']))
                    val = offers
                elif(key=='No of Images'): val = len(json_data['media']['albums'][0]['images'])
                elif(key=='Image URL'): val = json_data['media']['albums'][0]['images'][0]['imageURL'].replace('u002F','/')
                elif(key=='In Stock'): val = "Yes" if json_data['flags']['outOfStock']==False else "No"
                elif(key=='Count of Ratings'): val = json_data['ratings']['totalCount']
                elif(key=='Count of Reviews'): val = json_data['ratings']['reviewInfo']['reviewsCount']
                elif(key=='Seller URL'): val = json_data['sellers'][0]['sellerName']
                elif(key=='Color') : val = json_data['baseColour']
                elif(key == 'No of Colors'): val =len([json_data['baseColour']])
                elif(key == 'COD'): val = ''
                
                elif(key=='New Colum'):
                    val = json_data['Key for which the data is present']



            #If there is no data then the blank value will be assigned for particular column       
            except: val = ''
            temp_dict.update({key: val})
            val = ''
        #These are the columns names where data is not present in Myntra PDP page
        not_available_col = ['Current_Size', 'Bestseller Rank',
                                'Rank Detail','Ques','Description']
        for empty_col in not_available_col: temp_dict.update({empty_col: ''})
        # Filtration 
        try: temp_dict['Product Rating'] = round(temp_dict['Product Rating'],1)#This convert large floating value to smaller one
        except: pass
        print("**********************************")
        
        
        # This condition will check if all the mandatory fileds are present if not then the Product Id will be moved to Error ID's:
        if(temp_dict['Title']!='' and temp_dict['Brand']!='' and temp_dict['Image URL']!=''
           and temp_dict['Selling Price']!='' and temp_dict['MRP']!='' and temp_dict['Count of Ratings']!=''
           and temp_dict['Product Rating'] and temp_dict['In Stock']!='No'):
            print(temp_dict)
            all_output_lis.append(temp_dict)
        #If condition fails for mandatory tags data missing then id's will go in error directory
        else:
            Exception_URL.append(response.url)
        Data_without_filtered.append(temp_dict)
        return temp_dict #returns all the data either it might be mandatory or non mandatory tags
    #Ids will go into error if the product is not there in page
    except:
         Exception_URL.append(response.url)
         Error_Ids.append(int(re.findall(r'\d{5,9}', response.url)[0]))
         No_json_loads.append(int(re.findall(r'\d{5,9}', response.url)[0]))
        



#This is the main class to handle the structure of python scrapy, and also the first itteration
class MyntrapdpScrappingSpider(scrapy.Spider):
    #Customs settings are decalred here
    custom_settings = {
            'DOWNLOAD_DELAY': 1,
            'AUTOTHROTTLE_ENABLED': True,
        }    # Global Flow 
    name = 'MyntraPDP_Scrapping'
    allowed_domains = ['myntra.com']
    main_url = "https://www.myntra.com/"
    #This is the input file name and sheetname for which we want to scrape from the Input folder, we need to change if there is different file name arrives in future    
    productId_df = pd.read_excel(r"{}\\BK_Tier_1_Scraping_Input.xlsx".format(input_dir), sheet_name='Myntra')

    #Column name should be specified from the above sheet and it will convert those each ids into URL's
    productId_df['Input'] = main_url + productId_df['Input'].astype(str) #reading the product Id's from ProductId Excel
    #All product URLs are listed in all_urls
    all_urls = productId_df['Input']

    #This below function works as the sending requests to the web page for each url and sends the response to the parse function.
    def start_requests(self):
        #clearing the error directory if there is any data present before
        try:
            if(len(pd.read_excel(r"{}\\Myntra_ErrorIds.xlsx".format(error_dir)))>0):
                pd.DataFrame({'Exception_Id': []}).to_excel(r"{}\\Myntra_ErrorIds.xlsx".format(error_dir))
        except:pass
        count = 0
        #Loops all the urls and gets the response from the webpage 
        for each_url in self.all_urls[0:20]:
            # print(each_url)
            count+=1
            print("\n\n===================",count,"======================\n\n")
            #Callback function is Parse function where all the data is extracted using logics applied
            yield scrapy.Request(url = each_url, callback = parse, meta={"ASIN": each_url, 'handle_httpstatus_all': True, 
                                                                    
                                                                         },
                                  headers={"User-Agent": "My UserAgent"})

#This is the first itteration crawling.
process = CrawlerProcess(settings={'LOG_LEVEL': 'DEBUG',
                                   'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                                   'FEEDS': {"MyntraPDP1.csv": {'format': 'csv', 'overwrite': True}}
                                   })

#This class specifies the second itteration where ErrorIDs are the input here.
class MyntraErroRetrying(scrapy.Spider):
    main_url = "https://www.myntra.com/"
    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'AUTOTHROTTLE_ENABLED': True,
    }
    name='Error_running'
    #Through this function error IDs are converted into urls and sent back to parse function to get the data
    def start_requests(self):
        for error_id in Error_Ids:
            url_gen = self.main_url + str(error_id)
            yield scrapy.Request(url = url_gen, meta = {"ASIN": url_gen,'handle_httpstatus_all': True}, callback=parse)
error_proccess = CrawlerProcess(settings={'LOG_LEVEL': 'DEBUG',
                                   'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                                   'FEEDS': {"MyntraPDP2.csv": {'format': 'csv', 'overwrite': True}}
                                   })

#This logic manages to run the crawler multiple times to get the more accurate data.
error_proccess.crawl(MyntraErroRetrying)
configure_logging()
runner = CrawlerRunner()
@defer.inlineCallbacks
#This function firstly calls MyntrapdpScrappingSpider class and then MyntraErroRetrying
def crawl():
    yield runner.crawl(MyntrapdpScrappingSpider)
    yield runner.crawl(MyntraErroRetrying)
    reactor.stop()
crawl()
reactor.run()

#Further completing all the extration all the data will be stored in below variable
Original_df = pd.DataFrame(all_output_lis)

Output_prd_id = list(Original_df['Product ID'])
main_exp_id = []
#loop to check if the extracted data contains the Error ID or not
for exp_id in set(Error_Ids):
    Error_Ids =[]
    #If Error ID not in scrapped output data then consider that ID as error
    if exp_id not in Output_prd_id:
        # print("Exception ID:", exp_id)
        main_exp_id.append(exp_id)
#This will save the error file in Excecptoin directory
Exception_Df = pd.DataFrame({'Exception_ProductID':list(set(main_exp_id))}).to_excel(r"{}\\Myntra_ErrorIds.xlsx".format(error_dir))


#Function to clean mandatory Scrapped data
def postProcessing(Product_data):
    df = pd.DataFrame(Product_data)
    #Removes the duplicated occured from multiple crawler process
    df.drop_duplicates(subset='Product ID', keep='last', inplace=True)
    try:
        df['Product Details'] = df['Product Details'].str.replace("u003Cp","")
        df['Product Details'] = df['Product Details'].str.replace("u003E","")
        df['Product Details'] = df['Product Details'].str.replace("u003Cbr","")
        df['Product Details'] = df['Product Details'].str.split(",")
    except:
        pass
    #Output data for mandatory data will be saved as per the below file name given
    df.to_excel(r'{}\\Myntra_MandatoryOutput.xlsx'.format(output_dir), index=False)
    return df

#Function to clean non-mandatory Scrapped Data
def postProcessingNonMandatory(Product_data):
    df = pd.DataFrame(Product_data)
    #Removes the duplicated occured from multiple crawler process

    df.drop_duplicates(subset='Product ID', keep='last', inplace=True)
    try:
        df['Product Details'] = df['Product Details'].str.replace("u003Cp","")
        df['Product Details'] = df['Product Details'].str.replace("u003E","")
        df['Product Details'] = df['Product Details'].str.replace("u003Cbr","")
        df['Product Details'] = df['Product Details'].str.split(",")
    except:
        pass
    df.drop_duplicates(subset=['Product ID'], inplace = True)
    #Output data for Allproduct ID data will be saved as per the below file name given
    
    df.to_excel(r'{}\\Myntra_Output_AllData.xlsx'.format(output_dir), index=False)
    return df


pd.DataFrame(Data_without_filtered).to_excel(r"{}\\Ajio_NoFilteredData.xlsx".format(no_filtered_dir), index=False)
#Mandatory and All product ID data will be sent to postprocessing
Mandatory_data = postProcessing(Original_df)
NonMandatory_data = postProcessingNonMandatory(Data_without_filtered)


time.sleep(30)
#This function differentiate Alldata and mandatory to convert the non-mandatory data
try:
    mandatory_data = pd.read_excel(r"{}\\Myntra_MandatoryOutput.xlsx".format(output_dir))
    all_data = pd.read_excel(r"{}\\Myntra_Output_AllData.xlsx".format(output_dir))
    mand_prdId = mandatory_data['Product ID']
    for each_id in mand_prdId:
        all_data = all_data[all_data['Product ID']!=each_id]
    all_data.drop_duplicates(subset = 'Product ID', inplace=True)
    all_data.to_excel(r"{}\\Myntra_Filtered_NonMandatory.xlsx".format(output_dir), index=False)
except: all_data = ''
print("\n\n\n\nLength of Error IDs",len(Error_Ids),"\n\nmandatory data",len(all_output_lis),"\n\nNon Mandatory data",len(Data_without_filtered))

#If the product ID fails to load the backend data then those IDs will save as below file name
No_json_excep = pd.DataFrame({'Exception_ProductID':list(set(No_json_loads))}).to_excel(r'{}\\MyntraFailedJson_ErrorIds.xlsx'.format(error_dir))




#This will push the mandatory data to the GCP


#The below line of code pushes the Non-mandatory data to the GCP.




    
