import scrapy
import pandas as pd
import datetime
from requests_html import HTMLSession
import re
import json
from cleantext import clean
from urllib.parse import urlparse
from scrapy.crawler import CrawlerProcess
from selenium import webdriver
from selenium.webdriver.common.by import By
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




product_data_list =[]
error_url =[]
exception_id = []
Exception_URL = []
trying_url = []
all_output_lis = []
Error_Ids = []
Data_without_filtered = []
No_json_loads = []

def parse(response):
    try:
        if(len(pd.read_excel(r"Myntra_ErrorIds.xlsx"))>0):
            pd.DataFrame({'Exception_Id': []}).to_excel(r'Myntra_ErrorIds.xlsx')
    except:pass
    global Error_Ids, Exception_URL, Data_without_filtered, all_output_lis, No_json_loads

    parent_page = str(response.body)
    main_page = str(response.body)

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
    except:
        No_json_loads.append(int(re.findall(r'\d{5,9}', response.url)[0]))
    try:
        json_data = json.loads(json_data_str)#It will convert the string into json data, so that we can aceess items easily
        json_data = json_data['pdpData']#starting point of json string
        with open('myntrapdp.txt','w') as f:
            f.write(json_data_str)
        try:json_division = json.loads(json_division_str)
        except: pass
        #Below mentioned strings are the columns names we are setting here
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
                #Data will extract from the json file converted above if data is not found then balnk data will be assigned for each column name of each ProductID
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
            # data_temp_dict = {}
            print(temp_dict)
            # data_temp_dict = temp_dict
            all_output_lis.append(temp_dict)
            # print("\n\n\n\n\n", temp_dict,"\n\n\n\n\n\n")
        #If condition fails for mandatory tags data missing then id's will go in error directory
        else:
            # pass
            Exception_URL.append(response.url)
            Error_Ids.append(int(re.findall(r'\d{5,9}', response.url)[0]))
        # print("\n\n\n\n\n",temp_dict['title'])
        # all_output_lis.append(temp_dict)
        Data_without_filtered.append(temp_dict)
        return temp_dict #returns all the data either mandatory or non mandatory tags
    #Ids will go into error if the product is not there in page
    except:
         Exception_URL.append(response.url)
         Error_Ids.append(int(re.findall(r'\d{5,9}', response.url)[0]))
         No_json_loads.append(int(re.findall(r'\d{5,9}', response.url)[0]))
        




class MyntrapdpScrappingSpider(scrapy.Spider):
    custom_settings = {
            'DOWNLOAD_DELAY': 1,
            'AUTOTHROTTLE_ENABLED': True,
        }    # Global Flow 
    name = 'MyntraPDP_Scrapping'
    allowed_domains = ['myntra.com']
    main_url = "https://www.myntra.com/"
    #Converting all id's into list 
    # productId_df = pd.read_excel("MyntraPDP_ProductID.xlsx", sheet_name='Batch 1')
    #making url using each Product ID
    productId_df = pd.read_excel("New_Latest_Input_platformwise.xlsx", sheet_name='Myntra')
    productId_df['Input'] = main_url + productId_df['Input'].astype(str) #reading the product Id's from ProductId Excel

    all_urls = productId_df['Input']
    #Working product IDs  == 10787904 10787904

    def start_requests(self):
        try:
            if(len(pd.read_excel(r"Myntra_ErrorIds.xlsx"))>0):
                pd.DataFrame({'Exception_Id': []}).to_excel(r'Myntra_ErrorIds.xlsx')
        except:pass
        # print("\n\n\n\n",len(self.all_urls[0:200]),self.all_urls[0:200],"\n\n\n\n")
        for each_url in self.all_urls:
            # print(each_url)
            yield scrapy.Request(url = each_url, callback = parse)

process = CrawlerProcess(settings={'LOG_LEVEL': 'DEBUG',
                                   'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                                   'FEEDS': {"MyntraPDP1.csv": {'format': 'csv', 'overwrite': True}}
                                   })
process.crawl(MyntrapdpScrappingSpider)
process.start() 

Original_df = pd.DataFrame(all_output_lis)
Output_prd_id = list(Original_df['Product ID'])
main_exp_id = []
for exp_id in set(Error_Ids):
    Error_Ids =[]
    if exp_id not in Output_prd_id:
        # print("Exception ID:", exp_id)
        main_exp_id.append(exp_id)
Exception_Df = pd.DataFrame({'Exception_ProductID':list(set(main_exp_id))}).to_excel(r'Myntra_ErrorIds.xlsx')



def postProcessing(Product_data):
    df = pd.DataFrame(Product_data)
    df.drop_duplicates(subset='Product ID', keep='last', inplace=True)
    try:
        df['Product Details'] = df['Product Details'].str.replace("u003Cp","")
        df['Product Details'] = df['Product Details'].str.replace("u003E","")
        df['Product Details'] = df['Product Details'].str.replace("u003Cbr","")
        df['Product Details'] = df['Product Details'].str.split(",")
    except:
        pass
    df.to_excel(r'Myntra_MandatoryOutput.xlsx', index=False)
    return df


def postProcessingNonMandatory(Product_data):
    df = pd.DataFrame(Product_data)
    df.drop_duplicates(subset='Product ID', keep='last', inplace=True)
    try:
        df['Product Details'] = df['Product Details'].str.replace("u003Cp","")
        df['Product Details'] = df['Product Details'].str.replace("u003E","")
        df['Product Details'] = df['Product Details'].str.replace("u003Cbr","")
        df['Product Details'] = df['Product Details'].str.split(",")
    except:
        pass
    df.drop_duplicates(subset=['Product ID'], inplace = True)
    
    df.to_excel(r'Myntra_Output_AllData.xlsx', index=False)
    return df

# postProcessing(Original_df)
# postProcessingNonMandatory(Data_without_filtered)

Mandatory_data = postProcessing(Original_df)
NonMandatory_data = postProcessingNonMandatory(Data_without_filtered)


time.sleep(30)
try:
    mandatory_data = pd.read_excel("Myntra_MandatoryOutput.xlsx")
    all_data = pd.read_excel("Myntra_Output_AllData.xlsx")
    mand_prdId = mandatory_data['Product ID']
    for each_id in mand_prdId:
        all_data = all_data[all_data['Product ID']!=each_id]
    all_data.drop_duplicates(subset = 'Product ID', inplace=True)
    all_data.to_excel("Myntra_Filtered_NonMandatory.xlsx", index=False)
except: all_data = ''
# scraped_data = pd.DataFrame(Data_without_filtered)
# scraped_data.to_excel("OutputScrapped.xlsx", index=False)

# mandatory_data = pd.DataFrame(all_output_lis).to_excel("Myntra_MandatoryData.xlsx",index=False)

# error_ids = pd.DataFrame(Error_Ids, columns=['Error URL']).to_excel('Myntra_ErrorIds.xlsx', index= False)
print("\n\n\n\nLength of Error IDs",len(Error_Ids),"\n\nmandatory data",len(all_output_lis),"\n\nNon Mandatory data",len(Data_without_filtered))
No_json_excep = pd.DataFrame({'Exception_ProductID':list(set(No_json_loads))}).to_excel(r'MyntraFailedJson_ErrorIds.xlsx')






    
