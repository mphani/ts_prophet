import requests
import os
import json

from datetime import datetime
from datetime import timedelta
from datetime import date

DEFAULT_PROM_URL = "http://192.168.200.242:9090/api/v1/query_range"
prometheus_url_to_get = os.environ.get( "PROMETHEUS_URL" , DEFAULT_PROM_URL)

G_LOCAL_DATA_FOLDER="metrics_data"

G_QUERY_MAP ={
    "aerospike_namespace_client_write_success": 
        { "simple_query":"aerospike_namespace_client_write_success" }, 
        
    "aerospike_namespace_client_read_success": {
        "rate_query":"rate(aerospike_namespace_client_read_success[1m])" ,
        "simple_query": "aerospike_namespace_client_read_success"
        },
}

class PrometheusDataReader:
    def __init__(self, p_prom_url: str,  ):
        self.prometheus_url= self.__getPrometheusURL( p_prom_url)
    
    def __get_query_from_map(self, p_query_name: str, p_sub_query_name: str,):
        if p_query_name in G_QUERY_MAP:
            l_sub_queries_dict = G_QUERY_MAP[ p_query_name]
            return p_query_name, l_sub_queries_dict[p_sub_query_name ]
        
        return p_query_name
    
    def __fetch_data( self, p_query_name: str, p_sub_query_name: str, p_start: int, p_end: int, p_step: str = "1m"):
        # http://192.168.200.242:9090/api/v1/query_range?query=aerospike_namespace_client_write_success&start=1702274598&end=1702879398&step=1m
        
        l_query_name, l_query = self.__get_query_from_map( p_query_name, p_sub_query_name) 
        l_params={
            "query": l_query,
            "start": p_start,
            "end": p_end,
            "step": p_step
        }
        
        l_prometheus_url= self.prometheus_url
        
        # http_response = requests.get(prometheus_url_to_get, params={'query': p_query}, verify=False)    
        http_response = requests.get( l_prometheus_url, params=l_params, verify=False)    
        if "data" in http_response.json():
            data = http_response.json()["data"]
            data["query_name"] = p_query_name
            data["sub_query_name"] = p_sub_query_name
            data["prom_query"] = l_query
            # print( "\n**********\n",type(data),"***********\n",l_query)
            l_json_dict = json.dumps(data, indent=4)
            return l_json_dict

        # data = http_response.json()["data"]
        # return data

        print("unable to find [data] element in response for query: ", p_query_name,"\n")
        return ""
    
    def pulldata( self, p_query_name: str, p_sub_query_name: str, p_start: int, p_end: int, p_step: str = "1m"):
        l_data = self.__fetch_data( p_query_name, p_sub_query_name, p_start, p_end, p_step)
        if len(l_data)==0:
            return
        
        # save data
        self.__storeMetricsToFile(p_query_name, p_sub_query_name, p_start,p_end, l_data)   
        
    def pulldaywisedata(self, p_query_name: str, p_sub_query_name: str, p_from_date: str, p_to_date: str, p_step: str = "1m"):
        
        l_start_date = datetime.strptime(p_from_date, "%m/%d/%Y")
        if not p_to_date or len(p_to_date)==0:
            l_today = datetime.date()
        else:
            l_today = datetime.strptime(p_to_date, "%m/%d/%Y")
        
        # looping variables
        l_end_date = l_today
        l_iter_date= l_start_date
        while l_iter_date <= l_end_date:                    
            # l_for_date_starttime = datetime.strptime(l_iter_date, '%m/%d/%Y')
            l_for_date_starttime = l_iter_date
            l_for_date_endtime = l_for_date_starttime + timedelta( seconds= ((24*60*60)-1) )
            
            l_utc_starttime = int(l_for_date_starttime.strftime("%s"))
            l_utc_endtime = int(l_for_date_endtime.strftime("%s"))
            print( l_for_date_starttime, " : ", l_utc_starttime, "\t", l_for_date_endtime, " : ", l_utc_endtime)
            l_iter_date = l_iter_date + timedelta( days=1 )
            
            l_data = self.pulldata( p_query_name, p_sub_query_name, l_utc_starttime, l_utc_endtime, p_step)
            # print( l_data)
    
    def __get_filename(self, p_query_name: str, p_sub_query_name: str,p_start: int, p_end: int):
        l_startdate = datetime.strftime( datetime.fromtimestamp( p_start), "%d%m%Y" )
        
        l_name = p_query_name
        
        if len(p_sub_query_name)>0:
            l_name= p_query_name+"_"+p_sub_query_name
        
        return G_LOCAL_DATA_FOLDER +"/json/" + l_startdate +"_" + l_name +".json"
        
        # return G_LOCAL_DATA_FOLDER +"/json/" + str(p_start) +"_" + str(p_end) +"_" + p_query +".json"
    
    def __storeMetricsToFile(self, p_query_name: str, p_sub_query_name: str, p_start: int, p_end: int, p_output: str):
        l_filename = self.__get_filename( p_query_name, p_sub_query_name, p_start, p_end)
        # l_data_str = json.loads(p_output)
        print( l_filename)
        with open(l_filename, "w") as outfile:
            outfile.write(p_output)        

    def __getPrometheusURL(self, p_prom_url: str):
        l_prometheus_url= p_prom_url
        if not l_prometheus_url or len(l_prometheus_url)==0:
            l_prometheus_url= prometheus_url_to_get

        return l_prometheus_url

# class MetricsData(object):
#     def __init__(self, p_date, p_value ):
#         self.date= p_date
#         self.value = p_value

class JsonToCsvConvertor:
    def __init__(self, p_folder: str="", p_date: str = "" ):
        self.parent_folder = p_folder or G_LOCAL_DATA_FOLDER+"/json/"
        self.csv_filename = self.__get_filename( p_date)
    
    def save_to_csv(self, p_csv_lines: list, p_file_open_mode: str ="a"):
        with open( self.csv_filename, p_file_open_mode) as outfile:
            # outfile.writelines( "\n".join(p_csv_lines))
            outfile.writelines( p_csv_lines)
    
    def convert_json_to_csv(self, p_folder: str=""):
        l_folder = p_folder or self.parent_folder
        l_file_list = os.listdir(l_folder)
        # cluster_name  , instance , ns , service , metric_name , UTC-date-time, metric-value
        
        l_tmp_header = []
        # l_tmp_header.append("cluster_name,instance,ns,service,metric_name,utc_date_time,year,month,date,hour,minute,second,metric_value\n")
        l_tmp_header.append("cluster_name,instance,ns,service,metric_name,sub_query_name,prom_query,utc_date_time,metric_value\n")
        self.save_to_csv( l_tmp_header,"w")
        
        for l_file in l_file_list:
            l_file_lines = self.load_parse_json(l_file)
            self.save_to_csv( l_file_lines)
            # dump file-lines to a file
            
    def __get_filename(self, p_date: str=""):
        l_date = p_date
        if len(p_date)==0:
            l_date = datetime.strftime( datetime.now(),"%d%m%Y")
        
        return G_LOCAL_DATA_FOLDER+"/"+l_date+"_"+"aerospike_stats.csv"
            
    def load_parse_json(self, p_filename: str):
        l_contents=""
        l_filename = p_filename

        if not l_filename.startswith(G_LOCAL_DATA_FOLDER+"/json/"):
            l_filename = G_LOCAL_DATA_FOLDER+"/json/" + l_filename
        
        print("Parsing json file ", l_filename)
        with open(l_filename, "r") as outfile:
            l_contents = outfile.read()
            
        l_lines = self.__parse_json_as_csv(l_contents)
        
        return l_lines

    def __parse_json_as_csv(self, l_contents: str):
        l_json_path = json.loads( l_contents)
        l_lines = []
        # json tree-path
        # data:
        #   resultType
        #   result: Array [ {metric, values}]
        #       [0]:
        #           metric: <aerospike_namespace_client_write_success>
        #           values: [Array-of-Array]
        #               [0]:
        #                   0: <utc-date>
        #                   1: <metric-value>
        # with above structure, lets parse
        l_results_arr = l_json_path["result"]
        l_json_query_name = "NO_QUERY_NAME"
        l_sub_query_name ="NO_SUB_QUERY"
        l_prom_query ="NO_PROM_QUERY"
        if "query_name" in l_json_path:
            l_json_query_name = l_json_path["query_name"]
            l_sub_query_name = l_json_path["sub_query_name"]
            l_prom_query = l_json_path["prom_query"]
            
        # Header
        # cluster_name  , instance , ns , service , metric_name , UTC-date-time, metric-value
        for l_result in l_results_arr:
            # Metric
            if "__name__" in l_result["metric"]:                
                l_metric_name = l_result["metric"]["__name__"]
            else:
                l_metric_name = l_json_query_name
                
            l_cluster_name = l_result["metric"]["cluster_name"] or ""
            l_instance = l_result["metric"]["instance"] or ""
            l_ns = l_result["metric"]["ns"] or ""
            l_service = l_result["metric"]["service"] or ""
            # Value
            l_values = l_result["values"]
            for l_val in l_values:
                l_line = l_cluster_name +"," + l_instance +"," + l_ns +"," + l_service +"," + l_metric_name+","
                l_line = l_line + l_sub_query_name +"," + l_prom_query + ","

                # (l_year, l_month, l_day, l_hour, l_minute, l_second)= self.__get_time_from_utc(l_val[0])
                l_line = l_line + str(l_val[0])
                l_line = l_line + "," + str(l_val[1])
                
                l_lines.append( l_line+"\n")
                
        return l_lines
    
    def __get_time_from_utc(self, p_utc_timestamp):
        l_datetime = datetime.fromtimestamp(p_utc_timestamp)
        l_year = l_datetime.year
        l_month = l_datetime.month
        l_day = l_datetime.day
        l_hour= l_datetime.hour
        l_minute = l_datetime.minute
        l_second = l_datetime.second
        return l_year, l_month, l_day, l_hour, l_minute, l_second
            

class DataProvider:        
    def __init__(self, p_url: str= "", p_folder: str="", p_start_date: str="", p_end_date: str="" ):
        self.prometheus_url = p_url or prometheus_url_to_get 
        self.parent_folder = p_folder or G_LOCAL_DATA_FOLDER+"/json/"
        self.start_date = p_start_date or datetime.strftime( datetime.now(), "%m/%d/%Y")
        self.end_date = p_end_date or datetime.strftime( datetime.now(), "%m/%d/%Y")
        self.metrics = []
        # self.metrics = ["aerospike_namespace_client_write_success",
        #                 "aerospike_namespace_client_read_success",
        #                 ]
        
        for l_key in G_QUERY_MAP.keys():
            self.metrics.append( l_key)
        
    def generate(self, p_run_single_query: str=""):
        for l_query in self.metrics:
            if len(p_run_single_query)>0 and l_query== p_run_single_query:
                l_pdr = PrometheusDataReader( self.prometheus_url)
                l_queries_dict = G_QUERY_MAP[ l_query]
                for l_dict_sub_query in l_queries_dict.keys():
                    print( l_dict_sub_query)
                    l_pdr.pulldaywisedata( l_query, l_dict_sub_query, self.start_date, self.end_date)
            else:                       
                l_pdr = PrometheusDataReader( self.prometheus_url)
                # print( "start-date: ", self.start_date,"\tend-date: ", self.end_date)
                l_queries_dict = G_QUERY_MAP[ l_query]
                for l_dict_sub_query in l_queries_dict.keys():
                    print( l_dict_sub_query)
                    l_pdr.pulldaywisedata( l_query, l_dict_sub_query, self.start_date, self.end_date)
        
    
if __name__ == "__main__":
    print("Start: reading data from Prometheus ")
    # a = PrometheusDataReader(prometheus_url_to_get)
    # l_json_data = a.pulldata( "aerospike_namespace_client_write_success",1702274598,1702879398, "1m" )
    # print(l_json_data)
    # l_json_data = a.pulldata( "aerospike_namespace_client_read_success",1702274598,1702879398, "1m" )
    # print(l_json_data)
    # a.pulldaywisedata("aerospike_namespace_client_write_success", "12/01/2023", "12/17/2023")
    
    c = DataProvider(p_start_date="12/23/2023", )
    # c = DataProvider()
    c.generate()

    b = JsonToCsvConvertor( )
    b.convert_json_to_csv()
    
