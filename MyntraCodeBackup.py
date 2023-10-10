#Latest Scrapy file


import scrapy as sp
import pandas as pd
import datetime
import re
import json
from cleantext import clean
from urllib.parse import urlparse
from scrapy.crawler import CrawlerProcess
from twisted.internet import reactor, defer
from scrapy.utils.log import configure_logging
from scrapy.crawler import CrawlerRunner

main_url = "https://www.myntra.com/"

batch = 'Myntra' #Select which batch needs to be runed

#For Running Main batches pleae uncomment below line and comment line number 23
productId_df = pd.read_excel("New_Latest_Input_platformwise.xlsx", sheet_name='{}'.format(batch))
#For running error Files, please uncomment the below line and comment the above line
# productId_df = pd.read_excel(r"D:\Abhi\Web Scraping\AjioScrapping\ajio_Venv\MyntraPDP\MyntraPDP\spiders\OutputData\Exception_Url\MyntraPDP_Exception_URL_{}.xlsx".format(batch))

# all_urls = ['https://www.myntra.com/12357480']

productId_df['Input'] = main_url + productId_df['Input'].astype(str) #reading the product Id's from ProductId Excel
all_urls = productId_df['Input']



# productId_df = pd.read_excel("MyntraPDP_Exception_URL.xlsx")
# productId_df['Exception_URL'] = main_url + productId_df['Exception_URL'].astype(str)
# all_urls = productId_df['Exception_URL']
# all_urls = ['https://www.myntra.com/10787892?bm-verify=AAQAAAAG_____yId4NgL8VUyamBSRFY6ENX5oFyDxMtRye7fmeTWwRGX20hw7ojhOZwIwR9Em5Z6VxUWktx8uxSY0xx4nILBkOamvz4cYi7EWE4YZT4Z6a3VI-_Ww6fvbRwgkLxhtMotIP6wURu1EQ0EOJYx4LCNQT0MXLwkMGYLNl8pNKmNkXqMDhSgMP6x2AKHeBXD5TqaE0pjBAGS5xyEYVPW0nndwpd7IDIbsGOx6ZdF2yRFgg']
# all_urls=['https://www.myntra.com/10787900?bm-verify=AAQAAAAG_____8a0DOhULbzQvq3zeelMSmNnIuPc-x9r9iCndd458j2ORWlNKNMIncELnn7FTJ3r4xhHBlH6syRcpSGVBY9zs76BCFSzBMajsklrvRz-wH_tYcwVpZWNuCGBgr_Ii-swtkbI8Z7D3o6PHwf3fOAu80FEVICTwp7JKT-dJrNsWZQqAS6BuKeJKlvb5ptA7UhhPCbLHTpfnXmYyEIfH2XILOWlT8EJ4qIxXdOGH0a6zA']

#These variables are decalared for storing of data and manipulating them in required manner
exception_id = []
Exception_URL = []
trying_url = []
all_output_lis = []
Error_Ids = []
Data_without_filtered = []




#This is the main function which calls for each product Id, and it will extract all the necessary data from the
#Backend Json Data
def parse(response):
    try: 
        if(len(pd.read_excel(r"D:\Abhi\Web Scraping\AjioScrapping\ajio_Venv\MyntraPDP\MyntraPDP\spiders\OutputData\Exception_Url\MyntraPDP_Exception_ProductID.csv"))>0):
                pd.DataFrame({'Exception_Id': []}).to_excel(r'D:\Abhi\Web Scraping\AjioScrapping\ajio_Venv\MyntraPDP\MyntraPDP\spiders\OutputData\Exception_Url\MyntraPDP_Exception_ProductID.xlsx')
    except: pass
    global Error_Ids, Exception_URL, Data_without_filtered, all_output_lis

    parent_page = str(response.body)
    main_page = str(response.body)

    #As we are extracting data from backend so we need to decalred some start and end keywords where the data lies between
    #Below is the code which will consider the data laying point's
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
        print(temp_dict)
        
        # This condition will check if all the mandatory fileds are present if not then the Product Id will be moved to Error ID's:
        if(temp_dict['Title']!='' and temp_dict['Brand']!='' and temp_dict['Image URL']!=''
           and temp_dict['Selling Price']!='' and temp_dict['MRP']!='' and temp_dict['Count of Ratings']!=''
           and temp_dict['Product Rating'] and temp_dict['In Stock']!='No'):
            # data_temp_dict = {}

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


    
#This is the main building block of scrapy spider which runs initialls
class MyntrapdpScrappingSpider(sp.Spider):
    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'AUTOTHROTTLE_ENABLED': True,
    }
    # Global Flow 
    name = 'MyntraPDP_Scrapping'
    #this function will call for each Procut Ids and response will be sent to parse funtion
    
    def start_requests(self):
        try:
            if(len(pd.read_excel(r"D:\Abhi\Web Scraping\AjioScrapping\ajio_Venv\MyntraPDP\MyntraPDP\spiders\OutputData\Exception_Url\MyntraPDP_Exception_ProductID.csv"))>0):
                pd.DataFrame({'Exception_Id': []}).to_excel(r'D:\Abhi\Web Scraping\AjioScrapping\ajio_Venv\MyntraPDP\MyntraPDP\spiders\OutputData\Exception_Url\MyntraPDP_Exception_ProductID.xlsx')
        except: pass
        for url in all_urls:
           yield sp.Request(url = url ,callback=parse)        
        
   
      
process = CrawlerProcess(settings={'LOG_LEVEL': 'DEBUG',
                                   'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                                   'FEEDS': {"MyntraPDP_Data.csv": {'format': 'csv', 'overwrite': True}}
                                   })
process.crawl(MyntrapdpScrappingSpider)


Error_Ids = []
class MyntraScrapping_repetation(sp.Spider):
    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'AUTOTHROTTLE_ENABLED': True,
    }
    name = 'Rerunning_Error_batch'
    def start_requests(self):
        global Exception_URL, Error_Ids
        for error_url in Exception_URL:
            scrapped_len = len(all_output_lis)
            for loop in range(1):
                if(len(all_output_lis)>scrapped_len):break
                yield sp.Request(error_url, callback=parse)

error_looping = CrawlerProcess(settings={'LOG_LEVEL': 'DEBUG',
                                   'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                                   'FEEDS': {"MyntraPDP_ErrorOutputData.csv": {'format': 'csv', 'overwrite': True}}
                                   })
error_looping.crawl(MyntraScrapping_repetation)

configure_logging()
runner = CrawlerRunner()
@defer.inlineCallbacks
def crawl():
    yield runner.crawl(MyntrapdpScrappingSpider)
    yield runner.crawl(MyntraScrapping_repetation)
    reactor.stop()
crawl()
reactor.run()


#this will save the data file without filtering of mandatory fields.

original_df = pd.DataFrame(all_output_lis)
Output_prd_id = list(original_df['Product ID'])
# print("===================Originak Df", Output_prd_id)
main_exp_id = []
for exp_id in set(Error_Ids):
    Error_Ids =[]
    if exp_id not in Output_prd_id:
        # print("Exception ID:", exp_id)
        main_exp_id.append(exp_id)
Exception_Df = pd.DataFrame({'Exception_ProductID':list(set(main_exp_id))}).to_csv(r'D:\Abhi\Web Scraping\AjioScrapping\ajio_Venv\MyntraPDP\MyntraPDP\spiders\OutputData\Exception_Url\MyntraPDP_Exception_ProductID.csv')

#this function is used to filter the uncessary data occured during scarping
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
    # try:
    #     #This condition is wrote for error file directory, if we run error file then it will check if the data for particulary batch
    #     #is already present if yes then it will concat the will the older data.
    #     existing_batch = pd.read_excel(r"D:\Abhi\Web Scraping\AjioScrapping\ajio_Venv\MyntraPDP\MyntraPDP\spiders\OutputData\Myntra_scrapped_{}.xlsx".format(batch))
    #     if(existing_batch['Product ID']>1):
    #         df = pd.concat([existing_batch, df])
    #         df.to_excel(r'D:\Abhi\Web Scraping\AjioScrapping\ajio_Venv\MyntraPDP\MyntraPDP\spiders\OutputData\Myntra_scrapped_{}.xlsx'.format(batch), index=False)
    # except:
    # print(df)
    df.to_excel(r'D:\Abhi\Web Scraping\AjioScrapping\ajio_Venv\MyntraPDP\MyntraPDP\spiders\OutputData\Myntra_scrappedOutput.xlsx', index=False)

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
    # try:
    #     #This condition is wrote for error file directory, if we run error file then it will check if the data for particulary batch
    #     #is already present if yes then it will concat the will the older data.
    #     existing_batch = pd.read_excel(r"D:\Abhi\Web Scraping\AjioScrapping\ajio_Venv\MyntraPDP\MyntraPDP\spiders\OutputData\Myntra_scrapped_{}.xlsx".format(batch))
    #     if(existing_batch['Product ID']>1):
    #         df = pd.concat([existing_batch, df])
    #         df.to_excel(r'D:\Abhi\Web Scraping\AjioScrapping\ajio_Venv\MyntraPDP\MyntraPDP\spiders\OutputData\Myntra_scrapped_{}.xlsx'.format(batch), index=False)
    # except:
    # print(df)
    # df.drop_duplicates(subset=['Product ID'], inplace = True)
    # df.to_excel(r'D:\Abhi\Web Scraping\AjioScrapping\ajio_Venv\MyntraPDP\MyntraPDP\spiders\OutputData\Myntra_Output_NonMandatory.xlsx', index=False)

#Sending output data for data filtration
postProcessing(original_df)
postProcessingNonMandatory(Data_without_filtered)

try:

    mandatory_data = pd.DataFrame(all_output_lis)
    non_mandatory_data = pd.DataFrame(Data_without_filtered)
    for prdId in mandatory_data['Product ID']:
        non_mandatory_data = non_mandatory_data[non_mandatory_data['Product ID']!=prdId]
    non_mandatory_data.drop_duplicates(subset='Product ID', inplace=True)
    non_mandatory_data.to_excel(r"D:\Abhi\Web Scraping\AjioScrapping\ajio_Venv\MyntraPDP\MyntraPDP\spiders\OutputData\Myntra_UniqueNonmandatory.xlsx", index=False)
except:
    pass


